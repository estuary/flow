package materialize

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer"

	// Below are imports needed by the go sql package. These are not used directly, but they are
	// required in order to connect to the databases.
	// The sqlite driver
	_ "github.com/mattn/go-sqlite3"
	// The postgresql driver
	_ "github.com/lib/pq"
)

const (
	TargetTypePostgres string = "postgres"
	TargetTypeSqlite   string = "sqlite"

	MaterializationsTableName  string = "flow_materializations"
	OriginalDocumentColumnName string = "flow_document"
)

var PostgresSqlConfig SqlConfig = SqlConfig{
	IdentifierQuotes: TokenPair{
		Left:  "\"",
		Right: "\"",
	},
	DriverName: "postgres",
	GetSqlPlaceholder: func(i int) string {
		// The +1 here is because the i that's passed to this function is 0-indexed, but
		// postgres argument numbers start at 1.
		return fmt.Sprintf("$%d", i+1)
	},
}
var SqliteSqlConfig SqlConfig = SqlConfig{
	IdentifierQuotes: TokenPair{
		Left:  "\"",
		Right: "\"",
	},
	DriverName: "sqlite3",
	GetSqlPlaceholder: func(_ int) string {
		return "?"
	},
}

/*
* We expect the table:
*
* CREATE TABLE IF NOT EXISTS flow_materializations
* (
*     table_name PRIMARY KEY TEXT NOT NULL,
*     config_json TEXT NOT NULL
* );
*
* Where config_json is a json representation of a pf.CollectionSpec.
* This json holds information about the fields that will be materialized.
 */

// Holds SQL statements and related information about the target schema. This struct is created
// on initialization using data stored in the `flow_materializations` table in target database
type MaterializationSql struct {
	InsertStatement        string
	FullDocumentQuery      string
	RuntimeConfig          *pf.CollectionSpec
	ProjectionPointers     []string
	PrimaryKeyFieldIndexes []int
}

type TokenPair struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

type SqlConfig struct {
	IdentifierQuotes  TokenPair
	DriverName        string
	GetSqlPlaceholder func(int) string
}

func (self *SqlConfig) quoted(inner interface{}) quoted {
	return quoted{
		inner:  inner,
		quotes: &self.IdentifierQuotes,
	}
}

func getProjectionPointers(collection *pf.CollectionSpec) []string {
	var pointers []string
	for _, field := range collection.Projections {
		pointers = append(pointers, field.Ptr)
	}
	return pointers
}

// Loaded from Catalog database
type Materialization struct {
	CatalogDbId int32
	TargetName  string
	TargetUri   string
	TableName   string
	TargetType  string
}

func (self *Materialization) sqlConfig() (*SqlConfig, error) {
	switch self.TargetType {
	case TargetTypePostgres:
		return &PostgresSqlConfig, nil
	case TargetTypeSqlite:
		return &SqliteSqlConfig, nil
	default:
		return nil, fmt.Errorf("unsupported materialization target uri scheme: '%s'", self.TargetType)
	}
}

type quoted struct {
	quotes *TokenPair
	inner  interface{}
}

func (self *quoted) String() string {
	return fmt.Sprintf("%s%s%s", self.quotes.Left, self.inner, self.quotes.Right)
}

func NewMaterializationTarget(materialization *Materialization) (Target, error) {
	var sqlConfig, err = materialization.sqlConfig()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(sqlConfig.DriverName, materialization.TargetUri)
	if err != nil {
		return nil, err
	}
	runtimeConfig, err := loadRuntimeConfig(sqlConfig, db, materialization.TableName)
	if err != nil {
		return nil, err
	}

	var insertStatement = generateInsertStatement(materialization, runtimeConfig, sqlConfig)
	var documentQuery = generateFlowDocumentQuery(materialization, runtimeConfig, sqlConfig)
	log.WithFields(log.Fields{
		"targetType":      materialization.TargetType,
		"insertStatement": insertStatement,
		"documentQuery":   documentQuery,
	}).Info("Finished generating SQL for materialization")

	var projectionPointers []string
	var primaryKeyFieldIndexes []int
	for i, projection := range runtimeConfig.Projections {
		projectionPointers = append(projectionPointers, projection.Ptr)
		if projection.IsPrimaryKey {
			primaryKeyFieldIndexes = append(primaryKeyFieldIndexes, i)
		}
	}

	var sql = &MaterializationSql{
		RuntimeConfig:          runtimeConfig,
		InsertStatement:        insertStatement,
		FullDocumentQuery:      documentQuery,
		ProjectionPointers:     projectionPointers,
		PrimaryKeyFieldIndexes: primaryKeyFieldIndexes,
	}
	var store = consumer.NewSQLStore(db)
	return &MaterializationStore{
		sqlConfig: sql,
		delegate:  store,
	}, nil
}

func loadRuntimeConfig(sqlConfig *SqlConfig, db *sql.DB, tableName string) (*pf.CollectionSpec, error) {
	var sql = fmt.Sprintf("SELECT config_json FROM flow_materializations WHERE table_name = %s;", sqlConfig.GetSqlPlaceholder(0))
	log.WithFields(log.Fields{
		"tableName": tableName,
		"query":     sql,
	}).Debug("Loading materialization for table")
	var rows = db.QueryRow(sql, tableName)
	var runtimeConfigJson string
	var err = rows.Scan(&runtimeConfigJson)
	if err != nil {
		return nil, fmt.Errorf("Failed to query the materialization runtime configuration from the target database: %v", err)
	}
	var runtimeConf = new(pf.CollectionSpec)
	err = json.Unmarshal([]byte(runtimeConfigJson), runtimeConf)
	if err != nil {
		log.WithField("rawRuntimeConfiguration", runtimeConfigJson).
			WithField("tableName", tableName).
			Error("Failed to unmarshal materialization runtime configuration")
		return nil, fmt.Errorf("Materialization runtime configuration appears corrupted: %v", err)
	}
	return runtimeConf, nil
}

func generateFlowDocumentQuery(materialization *Materialization, runtimeConfig *pf.CollectionSpec, sqlConfig *SqlConfig) string {
	var tableName = sqlConfig.quoted(materialization.TableName)
	var conditions []string
	for _, field := range runtimeConfig.Projections {
		if field.IsPrimaryKey {
			var col = sqlConfig.quoted(field.Field)
			var condition = fmt.Sprintf("%s = %s", col.String(), sqlConfig.GetSqlPlaceholder(len(conditions)))
			conditions = append(conditions, condition)
		}
	}

	var columnName = sqlConfig.quoted(OriginalDocumentColumnName)
	return fmt.Sprintf("SELECT %s from %v WHERE %s;", columnName.String(), tableName.String(), strings.Join(conditions, " AND "))
}

func generateInsertStatement(materialization *Materialization, runtimeConfig *pf.CollectionSpec, sqlConfig *SqlConfig) string {
	var tableName = sqlConfig.quoted(materialization.TableName)

	var primaryKeyColumns []string
	quotedColumnNames := make([]string, len(runtimeConfig.Projections))
	var updateColumns []string
	for i, field := range runtimeConfig.Projections {
		// I don't know why, but go won't let you call String unless this is first
		// assigned to a variable. Perhaps some rules about the receiver being a pointer?
		var quotedCol = sqlConfig.quoted(field.Field)
		var quotedColumnName = quotedCol.String()
		quotedColumnNames[i] = quotedColumnName
		if field.IsPrimaryKey {
			primaryKeyColumns = append(primaryKeyColumns, quotedColumnName)
		} else {
			updateColumns = append(updateColumns, quotedColumnName)
		}
	}
	// Populate the placeholders for the query. These may be different depending on the driver.
	// +1 to i for the full document column
	var questionMarks = make([]string, len(runtimeConfig.Projections)+1)
	for i := 0; i < len(runtimeConfig.Projections)+1; i++ {
		questionMarks[i] = sqlConfig.GetSqlPlaceholder(i)
	}

	// We always add the column that holds the full document at the very end. This column needs to
	// be included in both the complete list and the list of columns that will be updated on a
	// unique constraint violation
	var fullDocumentColumn = sqlConfig.quoted(OriginalDocumentColumnName)
	var quotedFullDocColumn = fullDocumentColumn.String()
	quotedColumnNames = append(quotedColumnNames, quotedFullDocColumn)
	updateColumns = append(updateColumns, quotedFullDocColumn)

	var updates = make([]string, len(updateColumns))
	for i, uc := range updateColumns {
		updates[i] = fmt.Sprintf("%s = EXCLUDED.%s", uc, uc)
	}

	var onConflictDo = fmt.Sprintf("DO UPDATE SET %s", strings.Join(updates, ", "))

	var sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) %s;", tableName.String(), strings.Join(quotedColumnNames, ", "), strings.Join(questionMarks, ", "), strings.Join(primaryKeyColumns, ", "), onConflictDo)
	log.WithField("sql", sql).WithField("materialization", materialization.TargetName).Info("Generated SQL insert statement for materialization")
	return sql
}
