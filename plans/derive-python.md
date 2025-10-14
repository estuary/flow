# derive-python Implementation Plan v2

## Overview

Build a Python derivation connector for Estuary Flow that allows users to write async Python classes with Pydantic-typed transforms. This connector follows the established `derive-typescript` pattern and integrates seamlessly with Flow's distributed runtime.

Users write Python code that extends a generated base class, with full IDE support via generated Pydantic models. The connector handles protocol translation, type generation, validation, and subprocess management.

## Architecture

```
Flow Runtime (protocol messages via stdin/stdout)
    ↓
derive-python (Rust connector)
    - Handles Spec/Validate/Open requests
    - Maps JSON Schema → doc::Shape → AST → Pydantic models
    - Validates with pyright
    - Spawns Python via uv
    ↓
Python Runtime (asyncio event loop)
    - main.py (generated protocol wrapper)
    - types.py (generated Pydantic models)
    - module.py (user implementation)
```

## Key Design Decisions

### User Interface

**Module format**: Inline Python source or file URL
```yaml
derive:
  using:
    python: { module: "inline python code..." }
    # or
    python: { module: "path/to/module.py" }
```

**Class-based API**: User extends generated `IDerivation` base class
```python
from patterns.sums import IDerivation, Document, SourceFromInts

class Derivation(IDerivation):
    def __init__(self, open: Open):
        super().__init__(open)
        self.state = open.state  # Access persisted state

    async def from_ints(self, read: ReadFromInts) -> list[Document]:
        return [Document(Key=read.doc.Key, Sum=read.doc.Int)]

    async def flush(self) -> list[Document]:
        # Optional: emit buffered documents
        return []

    def start_commit(self, start_commit: StartCommit) -> StartedCommit:
        # Optional: return state update to persist
        return StartedCommit()

    async def reset(self):
        # Optional: reset state for testing
        pass
```

**Async-first**: All transform methods are `async def` for pipelining support. Users can start async tasks in transforms and await in `flush()` for advanced pipelining patterns.

**Pydantic V2**: Generated types use Pydantic models for runtime validation and IDE support.

**uv inline syntax**: Dependencies via PEP 723 `# /// script` blocks (no requirements.txt needed):
```python
# /// script
# dependencies = ["httpx>=0.27", "pydantic>=2.0"]
# requires-python = ">=3.14"
# ///
```

### Type Generation Pipeline

**Flow**: JSON Schema → `doc::Shape` → AST → Pydantic code

1. **JSON Schema → doc::Shape**: Use `doc::Shape::infer()` to convert JSON Schema into a flattened, non-recursive representation. `doc::Shape` provides:
   - `type_`: Type set (STRING, INTEGER, OBJECT, ARRAY, etc.)
   - `object.properties`: List of object properties with `is_required` flags
   - `array.tuple` / `array.additional_items`: Array shape info
   - `enum_`: Explicit value enumeration
   - `title` / `description`: Documentation
   - `provenance`: Tracks `$anchor` references

2. **doc::Shape → AST**: Intermediate AST representation (similar to derive-typescript):
   - Handles union types, nested objects, optional fields
   - Resolves `$anchor` references to top-level types
   - Tracks comments for docstrings

3. **AST → Pydantic**: Render Python code:
   - Object → Pydantic `BaseModel` with typed fields
   - Array → `list[T]` or `tuple[T1, T2, ...]`
   - Union types → `Union[T1, T2]` or `Optional[T]`
   - Enum → `Literal[val1, val2, ...]`
   - $anchor → Top-level Pydantic class with PascalCase name

**Package structure**: Generate proper Python packages with `__init__.py` files
```
flow_generated/
  python/
    patterns/
      __init__.py
      sums.py           # Generated types for patterns/sums collection
```

**Generated types**: For each derivation:
- `Document` model from collection write schema
- `Read{TransformName}` wrapper classes with `.doc` field for each transform
- `Source{Transform}` models from source collections
- `IDerivation` base class with `@abstractmethod` decorated async methods
- Protocol types: `Open`, `StartCommit`, `StartedCommit` for lifecycle hooks (minimal but accepting additional fields via `extra='allow'`)
- Top-level classes for `$anchor` schemas

**Key mappings** (JSON Schema → Python/Pydantic):
- `type: string` → `str`
- `type: integer` → `int`
- `type: number` → `float`
- `type: boolean` → `bool`
- `type: null` → `None`
- `type: array` → `list[T]` or `tuple[T1, T2, ...]` for fixed-length
- `type: object` → nested `BaseModel` or `dict[str, T]` for additionalProperties
- `required` array → required fields vs `Optional[T]`
- `enum` → `Literal[val1, val2, ...]`
- `$anchor` → top-level PascalCase class
- Union types → `Union[T1, T2, ...]` or `Optional[T]`

### Runtime

**Python version**: 3.14 (latest stable as of October 2025)

**Subprocess management**: Spawn via `uv run --python 3.14 --isolated main.py`
- `--isolated`: Ensures clean environment without user's global packages
- Sets `PYTHONPATH` to include `flow_generated/python` for imports

**Protocol**: Sequential message processing (await each). The main loop:
1. Reads Open message and instantiates user's Derivation class
2. Processes stdin line-by-line:
   - `read` → call transform method, emit published docs
   - `flush` → call flush(), emit docs, emit flushed response
   - `startCommit` → call start_commit(), emit startedCommit with optional state
   - `reset` → call reset() (testing only)

**Event loop**: Use `asyncio.run(main())` for simplicity and automatic cleanup.

**Error handling**: Fail transaction on exception. The runtime handles retries.

### Lambda Config & read_only Flag

Each transform has optional lambda configuration:
```json
{ "read_only": false }
```

**Purpose of `read_only`**: Informs the runtime whether a transform modifies internal state:
- `read_only: true`: Transform is stateless/pure. Documents can be sent to any connector instance for processing. Enables horizontal scale-out optimizations.
- `read_only: false` (default): Transform may modify state. Documents must be consistently routed to the same instance based on shuffle key. Ensures state consistency.

The flag is not currently enforced but enables future runtime optimizations for scale-out patterns.

## Implementation Phases

### Phase 1: Core Infrastructure

**Goal**: Minimal connector that responds to Spec/Validate/Open

**Tasks**:
1. Add `PYTHON = 5` to `flow.proto` ConnectorType enum
2. Regenerate protobufs: `make go-protobufs rust-protobufs`
3. Create `crates/models/src/derive_python.rs`:
   ```rust
   #[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
   #[serde(deny_unknown_fields, rename_all = "camelCase")]
   pub struct DeriveUsingPython {
       #[schemars(schema_with = "DeriveUsingPython::module_schema")]
       pub module: super::RawValue,
   }
   ```
4. Add `Python(DeriveUsingPython)` to `DeriveUsing` enum in `crates/models/src/derivation.rs`
5. Update `crates/models/src/lib.rs` to export `derive_python` module
6. Update `crates/runtime/src/derive/connector.rs::extract_endpoint()` to map `ConnectorType::Python` to connector config:
   ```rust
   } else if connector_type == ConnectorType::Python as i32 {
       Ok((
           models::DeriveUsing::Connector(models::ConnectorConfig {
               image: "ghcr.io/estuary/derive-python:dev".to_string(),
               config: serde_json::from_slice::<models::RawValue>(config_json)
                   .context("parsing connector config")?,
           }),
           config_json,
       ))
   ```
7. Create `crates/derive-python/Cargo.toml` with dependencies:
   - proto-flow
   - serde, serde_json
   - anyhow
   - doc (for Shape inference)
   - json (for schema types)
   - url, regex
   - tempfile
   - itertools
   - tracing
8. Create `crates/derive-python/src/main.rs` with logging setup
9. Create `crates/derive-python/src/lib.rs` with basic structure:
   - Config/LambdaConfig structs
   - `run()` function with stdin/stdout loop
   - Spec response (protocol 3032023, empty schemas)
   - Stub Validate/Open handlers

**Reference**: `crates/derive-typescript/src/lib.rs` for overall structure

### Phase 2: Code Generation

**Goal**: Generate Pydantic models and runtime wrapper from schemas

**Tasks**:
1. Create `crates/derive-python/src/codegen/mod.rs` with public functions:
   - `types_py()` - Generate Pydantic models
   - `main_py()` - Generate runtime wrapper from template
   - `stub_py()` - Generate stub implementation
2. Create `crates/derive-python/src/codegen/ast.rs`:
   - Define AST enum (Object, Array, Union, Literal, etc.)
   - Implement `render()` method to output Python code
   - Handle proper indentation, optional fields, docstrings
3. Create `crates/derive-python/src/codegen/mapper.rs`:
   - Implement `Mapper::new()` to build validator and extract anchors
   - Implement `Mapper::map()` to convert URL → Shape → AST
   - Implement `to_ast()` with shape-to-AST conversion logic:
     - Handle union types (multiple types in `type_` set)
     - Map objects → AST::Object with properties
     - Map arrays → AST::Array or AST::Tuple
     - Map enums → AST::Union with literals
     - Resolve $anchor references → AST::Anchor
   - Python variable name regex: `^[a-zA-Z_][a-zA-Z0-9_]*$`
   - Named schema anchor regex: `^[A-Z][\\w_]+$` (PascalCase)
   - Implement naming helper functions:
     - `to_pascal_case()`: For class names (SourceFromInts, ReadFromInts)
     - `to_snake_case()`: For method names (from_ints)
4. Implement `types_py()` to generate:
   - Import statements (Pydantic, typing)
   - `Document` model from collection write schema
   - For each transform:
     - `Source{Transform}` models from source collections
     - Top-level anchor classes
   - Protocol message types: `Open`, `StartCommit`, `StartedCommit` (minimal with `extra='allow'`)
   - `Read{Transform}` wrapper classes with `.doc: Source{Transform}` field only
   - `IDerivation` base class (extends `ABC`) with:
     - `__init__(self, open: Open)` constructor
     - `@abstractmethod` decorated async transform methods: `async def {transform_name}(self, read: Read{Transform}) -> list[Document]`
     - Default `async def flush(self) -> list[Document]` returning `[]`
     - Default `async def reset(self)` no-op
     - Default `def start_commit(self, start_commit: StartCommit) -> StartedCommit` returning empty
5. Create `crates/derive-python/src/codegen/main.py.template`:
   - PEP 723 dependencies block
   - Import asyncio, json, sys
   - Import generated types
   - Import user module
   - Parse Open message and instantiate Derivation class
   - Build array of transform method references
   - Async main loop reading stdin:
     - Parse JSON messages
     - Dispatch to transform/flush/startCommit/reset
     - Emit responses to stdout
   - Use `asyncio.run(main())`
6. Implement `main_py()` to render template with transform methods
7. Implement `stub_py()` for missing module URLs (file without whitespace):
   - Import types from generated module
   - Create stub Derivation class extending IDerivation
   - Each transform raises `NotImplementedError`
8. Implement package structure helper to generate `__init__.py` files for all parent directories

**Key Python generation patterns**:
- Use `BaseModel` with `Field()` for descriptions
- Optional fields: `field: Optional[Type] = None`
- Union types: `Union[Type1, Type2]` or `Type1 | Type2` (Python 3.10+)
- Literal enums: `Literal["val1", "val2"]`
- Nested objects: Inline class definitions or top-level classes
- Additional properties: Use `model_config = ConfigDict(extra='allow')` to accept extra fields
- Protocol types: Minimal models with `extra='allow'` to accept additional fields from protocol

**Reference**: `crates/derive-typescript/src/codegen/` for patterns

### Phase 3: Validation Logic

**Goal**: Validate user code and return generated files

**Tasks**:
1. Implement `validate()` function in `lib.rs`:
   - Parse config and transform lambda configs
   - Validate shuffle_lambda_config_json is empty (not supported yet)
   - Generate types via `codegen::types_py()`
   - Build `generated_files` map with full paths:
     - Types file: `{project_root}/flow_generated/python/{collection.name}.py`
       - Replace `/` in collection name with `/` for directory nesting
       - Generate `__init__.py` for each parent directory
     - Stub module if URL without whitespace
   - Write all files to temp directory
   - Set `PYTHONPATH` to `{temp_dir}/flow_generated/python`
   - Run syntax check: `python -m py_compile module.py`
   - Run type check: `pyright --pythonpath {temp_dir}/flow_generated/python`
   - Capture stderr and rewrite temp paths to project paths (skip for v1)
   - Return `Validated` response with transform `read_only` flags and `generated_files`

2. Helper function to compute all parent directories needing `__init__.py`:
   ```rust
   fn package_init_files(collection_name: &str) -> Vec<String> {
       // For "patterns/sums", return ["patterns/__init__.py"]
       // For "a/b/c", return ["a/__init__.py", "a/b/__init__.py"]
   }
   ```

**Error handling**: Return descriptive errors for:
- Invalid config JSON
- Python syntax errors
- Pyright type errors
- Missing dependencies

**Note**: Validation runs out-of-band from runtime, no subprocess needed at runtime

### Phase 4: Runtime Execution

**Goal**: Spawn Python and forward protocol messages

**Tasks**:
1. Implement Open handler in `lib.rs`:
   - Extract config and transforms from Open message
   - Create temp directory for execution
   - Generate and write types with package structure:
     - Main types file: `flow_generated/python/{collection.name}.py`
     - All `__init__.py` files for parent directories
   - Write user module
   - Generate and write runtime wrapper (`main.py`)
   - Spawn subprocess: `uv run --python 3.14 --isolated main.py`
     - Set working directory to temp dir
     - Set `PYTHONPATH` env var to include `flow_generated/python`
     - Pipe stdin/stdout
2. Forward Open message to subprocess stdin
3. Stream remaining stdin to subprocess
4. Stream subprocess stdout back to Flow runtime
5. Handle process lifecycle:
   - Wait for process exit
   - Propagate exit code
   - Log stderr for debugging
6. Error handling for subprocess failures

**Important**: Use `std::io::copy` for efficient streaming, similar to derive-typescript's approach

**Reference**: `crates/derive-typescript/src/lib.rs` lines 77-112 for subprocess management

### Phase 5: Testing & Examples

**Goal**: Comprehensive test coverage and working examples

**Tasks**:
1. Create `crates/derive-python/src/codegen/mapper_test.json` with test schemas:
   - Nested objects
   - Arrays and tuples
   - Union types
   - Enums
   - Optional fields
   - $anchor references
   - Additional properties
   - Pattern properties
2. Add unit test in `mapper.rs` using insta for snapshot testing:
   ```rust
   #[test]
   fn schema_generation() {
       // Load fixture, generate types, snapshot result
       insta::assert_snapshot!(generated_code);
   }
   ```
3. Create Python example in `examples/derive-patterns/`:
   - **Novel example**: Real-time event aggregation with time-based windows
     ```python
     # Event aggregator that computes rolling statistics over time windows
     # Demonstrates: async pipelining, state management, flush() usage
     ```
   - Show off capabilities:
     - Async transforms
     - State persistence via start_commit()
     - Pipelining with flush()
     - Pydantic validation
     - Type safety
4. Add corresponding `.flow.yaml` spec
5. Run `make catalog-test` to verify end-to-end
6. Verify IDE support:
   - Run `flowctl generate` to create types
   - Open in VSCode/PyCharm
   - Confirm autocomplete works
   - Confirm type checking works

**Example structure**:
```
examples/derive-patterns/
  event_aggregator.flow.yaml    # Collection specs
  event_aggregator.py            # Implementation
  flow_generated/                # Generated after flowctl generate
    python/
      patterns/
        event_aggregator.py      # Generated types
```

### Phase 6: Docker Image

**Goal**: Build production-ready Docker image

**Tasks**:
1. Create `crates/derive-python/Dockerfile`:
   ```dockerfile
   FROM python:3.14-slim

   # Install uv and pyright
   RUN pip install --no-cache-dir uv pyright

   # Copy Rust binary
   COPY target/x86_64-unknown-linux-musl/release/derive-python /

   ENTRYPOINT ["/derive-python"]
   LABEL FLOW_RUNTIME_CODEC=json
   LABEL FLOW_RUNTIME_PROTOCOL=derive
   ```
2. Update CI/CD to build and push image
3. Test image with example derivations

**Reference**: `crates/derive-typescript/Dockerfile`

## File Structure

```
crates/derive-python/
├── Cargo.toml
├── Dockerfile
├── src/
│   ├── main.rs                   # Entry point with tracing setup
│   ├── lib.rs                    # Spec/Validate/Open handlers
│   └── codegen/
│       ├── mod.rs                # Public API: types_py(), main_py(), stub_py()
│       ├── ast.rs                # AST types and Python rendering
│       ├── mapper.rs             # Schema → Shape → AST mapper
│       ├── mapper_test.json      # Test fixtures
│       └── main.py.template      # Async protocol wrapper template

crates/models/src/
├── derive_python.rs              # New: DeriveUsingPython
├── derivation.rs                 # Modified: add Python variant
└── lib.rs                        # Modified: export derive_python

crates/runtime/src/derive/
└── connector.rs                  # Modified: add Python case

go/protocols/flow/
└── flow.proto                    # Modified: add PYTHON = 5

examples/derive-patterns/
├── event_aggregator.flow.yaml    # Novel Python example
└── event_aggregator.py
```

## Integration Points

### Protobuf
Add to `CollectionSpec.Derivation.ConnectorType` enum:
```protobuf
enum ConnectorType {
    INVALID_CONNECTOR_TYPE = 0;
    SQLITE = 1;
    TYPESCRIPT = 2;
    IMAGE = 3;
    LOCAL = 4;
    PYTHON = 5;  // New
}
```

### Runtime Dispatch
In `connector.rs::extract_endpoint()`:
```rust
} else if connector_type == ConnectorType::Python as i32 {
    Ok((
        models::DeriveUsing::Connector(models::ConnectorConfig {
            image: "ghcr.io/estuary/derive-python:dev".to_string(),
            config: serde_json::from_slice::<models::RawValue>(config_json)?,
        }),
        config_json,
    ))
```

### Models
Add to `DeriveUsing` enum in `derivation.rs`:
```rust
pub enum DeriveUsing {
    Connector(ConnectorConfig),
    Sqlite(DeriveUsingSqlite),
    Typescript(DeriveUsingTypescript),
    Python(DeriveUsingPython),  // New
    Local(LocalConfig),
}
```

## Technical Notes

### Async Execution Model
- Protocol messages are processed sequentially (one `await` at a time in main loop)
- Users can start async tasks within transforms (e.g., `asyncio.create_task()`)
- Tasks can be awaited in `flush()` for pipelining:
  ```python
  async def transform(self, read):
      self.pending_tasks.append(asyncio.create_task(fetch_data(read.doc)))
      return []

  async def flush(self):
      results = await asyncio.gather(*self.pending_tasks)
      self.pending_tasks.clear()
      return [Document(...) for r in results]
  ```

### State Management
- Persisted state is passed via `Open.state_json`
- User accesses via constructor: `def __init__(self, open: Open)`
- Updates returned from `start_commit()`:
  ```python
  def start_commit(self, start_commit: StartCommit) -> StartedCommit:
      return StartedCommit(
          state=ConnectorState(
              updated_json=json.dumps({"counter": self.counter}),
              merge_patch=False  # Full replacement
          )
      )
  ```

### Package Structure for IDE Support
Generated files create proper Python packages:
```
flow_generated/python/
  patterns/
    __init__.py
    sums.py          # types for patterns/sums collection
```

User code imports: `from patterns.sums import IDerivation, Document`

### Shuffle Lambdas
Not supported initially (matches TypeScript):
```rust
if !shuffle_lambda_config_json.is_empty() {
    anyhow::bail!("computed shuffles are not supported yet");
}
```

### Python Naming Conventions

**Transform names** appear in two contexts, each following Python conventions:

1. **Class names** (for `Source{Transform}` and `Read{Transform}` types):
   - Use `to_pascal_case()`: Capitalize first letter after delimiters
   - Examples: `"fromInts"` → `"SourceFromInts"`, `"fetch-data"` → `"SourceFetchData"`

2. **Method names** (for transform methods in `IDerivation`):
   - Use `to_snake_case()`: Insert underscores at camelCase boundaries, lowercase all
   - Examples: `"fromInts"` → `"from_ints()"`, `"fetchData"` → `"fetch_data()"`

**Implementation**:
```rust
lazy_static::lazy_static! {
    static ref CAMEL_BOUNDARY: Regex = Regex::new(r"([a-z0-9])([A-Z])").unwrap();
}

fn to_pascal_case(name: &str) -> String {
    let mut result = String::new();
    let mut uppercase_next = true;

    for c in name.chars() {
        if !c.is_alphanumeric() {
            uppercase_next = true;
        } else if uppercase_next {
            result.extend(c.to_uppercase());
            uppercase_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

fn to_snake_case(name: &str) -> String {
    let with_boundaries = CAMEL_BOUNDARY.replace_all(name, "${1}_${2}");
    with_boundaries
        .chars()
        .map(|c| if c.is_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
        .collect::<String>()
        .split('_')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}
```

**Other naming**:
- **Collection names**: Use as-is for module paths
  - `"patterns/sums"` → `"patterns/sums.py"` (slashes create directory hierarchy)
- **$anchor names**: Keep PascalCase for class names
  - `"MyType"` → `"class MyType(BaseModel)"`

### PYTHONPATH Management
The subprocess must be able to import generated types:
- Set `PYTHONPATH` to `{temp_dir}/flow_generated/python`
- Or set working directory and use relative imports
- Ensure `__init__.py` files exist for all packages

## Success Criteria

1. ✅ User writes Python module (inline or file) with Derivation class
2. ✅ `flowctl generate` produces importable types in `flow_generated/python/`
3. ✅ IDE autocomplete and type checking work with generated types
4. ✅ Async transforms and flush() work correctly
5. ✅ uv resolves inline dependencies at runtime
6. ✅ State management via constructor and start_commit() works
7. ✅ Examples run end-to-end through catalog tests
8. ✅ Unit tests pass with snapshot coverage
9. ✅ Pyright type checking validates user code during Validate phase
10. ✅ Error messages are clear and actionable

## References

- `crates/derive-typescript/` - Primary reference for connector structure
- `crates/derive-sqlite/` - Simpler connector, good for validation patterns
- `crates/doc/src/shape/` - Shape inference and types
- `go/protocols/derive/derive.proto` - Protocol specification
- `examples/derive-patterns/` - Test patterns to replicate in Python
- [PEP 723](https://peps.python.org/pep-0723/) - Inline script metadata
- [Pydantic V2 docs](https://docs.pydantic.dev/latest/) - Type generation reference
- [uv docs](https://docs.astral.sh/uv/) - Script execution
- [Pyright docs](https://github.com/microsoft/pyright) - Type checking

## Development Workflow

1. **Phase 1**: Get basic protocol working (Spec response)
2. **Phase 2**: Implement type generation, verify output manually
3. **Phase 3**: Add validation with pyright, test error handling
4. **Phase 4**: Implement runtime execution, test with simple example
5. **Phase 5**: Create comprehensive example, add tests, verify IDE support
6. **Phase 6**: Build Docker image, integrate with CI/CD
7. **Polish**: Address edge cases, improve error messages, add documentation

## Notes for Implementer

- **Follow TypeScript patterns closely** - The derive-typescript implementation is battle-tested
- **Use existing abstractions** - `doc::Shape`, `doc::Validator` handle complex schema logic
- **Test incrementally** - Verify each phase works before moving to next
- **Snapshot test generated code** - Use `insta` crate for easy verification
- **Consider error messages** - Users will see these, make them helpful
- **Don't over-engineer** - Start with basics, add features as needed
- **Read the protocols** - Understanding derive.proto is essential
- **Check existing issues** - Look for TypeScript bugs/limitations to avoid

---

## Implementation Status

### Phase 1: Core Infrastructure ✅ COMPLETE (2025-10-11)

- Added `PYTHON = 5` to flow.proto and regenerated protobufs
- Created `crates/models/src/derive_python.rs` and integrated with DeriveUsing enum
- Updated runtime dispatcher to route Python derivations to connector image
- Built minimal connector responding to Spec/Validate/Open requests
- Fixed all non-exhaustive pattern matches in validation, sources, and flowctl crates
- Added test coverage in sources crate with Python derivation examples in test_derivations.yaml
- All sources tests pass with updated snapshots
- Entire workspace compiles successfully

**Next:** Phase 2 (Code Generation)
