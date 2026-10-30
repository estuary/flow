package azure

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/stores"
)

// adStore implements the Store interface for Azure Blob Storage
// using Azure AD authentication (azure-ad:// scheme)
type adStore struct {
	storeBase
	tenantID string // Tenant that owns the storage account.
	client   *service.Client

	// User delegation credentials are cached access tokens that
	// must be periodically refreshed using our main credentials.
	udc struct {
		mu    sync.Mutex
		exp   time.Time
		inner *service.UserDelegationCredential
	}
}

// NewAD creates a new Azure AD authenticated Store from the provided URL.
func NewAD(ep *url.URL) (stores.Store, error) {
	var args StoreQueryArgs

	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}

	var path = strings.Split(ep.Path[1:], "/")
	if len(path) < 2 {
		return nil, fmt.Errorf("azure-ad:// URL must include storage account and container: azure-ad://tenant-id/storage-account/container/prefix/")
	}

	var tenantID = ep.Host
	var storageAccount = path[0]
	var container = path[1]
	var prefix = strings.Join(path[2:], "/")

	var clientID = os.Getenv("AZURE_CLIENT_ID")
	var clientSecret = os.Getenv("AZURE_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("AZURE_CLIENT_ID and AZURE_CLIENT_SECRET must be set for azure-ad:// URLs")
	}

	var credentials, err = azidentity.NewClientSecretCredential(
		tenantID,
		clientID,
		clientSecret,
		&azidentity.ClientSecretCredentialOptions{
			DisableInstanceDiscovery: true,
		},
	)
	if err != nil {
		return nil, err
	}

	var refreshFn = func(credential azblob.TokenCredential) time.Duration {
		if token, err := credentials.GetToken(
			context.Background(),
			policy.TokenRequestOptions{
				TenantID: tenantID,
				Scopes:   []string{"https://storage.azure.com/.default"}},
		); err != nil {
			log.WithFields(log.Fields{
				"err":    err,
				"tenant": tenantID,
			}).Errorf("failed to refresh Azure credential (will retry)")

			return time.Minute
		} else {
			credential.SetToken(token.Token)
			return token.ExpiresOn.Sub(time.Now().Add(time.Minute))
		}
	}
	var accessKey = azblob.NewTokenCredential("", refreshFn)

	client, err := service.NewClient(
		azureStorageURL(storageAccount),
		credentials,
		&service.ClientOptions{},
	)
	if err != nil {
		return nil, err
	}

	var store = &adStore{
		storeBase: storeBase{
			storageAccount: storageAccount,
			container:      container,
			prefix:         prefix,
			args:           args,
			pipeline:       azblob.NewPipeline(accessKey, azblob.PipelineOptions{}),
		},
		tenantID: tenantID,
		client:   client,
	}

	log.WithFields(log.Fields{
		"tenant":         tenantID,
		"storageAccount": storageAccount,
		"container":      container,
		"prefix":         prefix,
	}).Info("constructed new Azure AD storage client")

	return store, nil
}

// SignGet returns a signed URL for GET operations using User Delegation Key
func (a *adStore) SignGet(path string, d time.Duration) (string, error) {
	var blob = a.args.RewritePath(a.prefix, path)

	var udc, err = a.fetchUserDelegationCredential()
	if err != nil {
		return "", err
	}

	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(d),
		ContainerName: a.container,
		BlobName:      blob,
		Permissions:   to.Ptr(sas.BlobPermissions{Read: true}).String(),
	}.SignWithUserDelegation(udc)

	if err != nil {
		return "", err
	}

	log.WithFields(log.Fields{
		"tenant":         a.tenantID,
		"storageAccount": a.storageAccount,
		"container":      a.container,
		"blob":           blob,
		"expires":        sasQueryParams.ExpiryTime(),
	}).Debug("Signed get request with user delegation")

	return fmt.Sprintf("%s/%s?%s", a.containerURL(), blob, sasQueryParams.Encode()), nil
}

func (a *adStore) fetchUserDelegationCredential() (*service.UserDelegationCredential, error) {
	a.udc.mu.Lock()
	defer a.udc.mu.Unlock()

	var now = time.Now()
	const DUR = time.Hour * 2

	// If the current token has at least half its duration left, then re-use it.
	if a.udc.exp.After(now.Add(DUR / 2)) {
		return a.udc.inner, nil
	}
	var exp = now.Add(DUR)

	var keyInfo = service.KeyInfo{
		Start:  to.Ptr(now.UTC().Format(sas.TimeFormat)),
		Expiry: to.Ptr(exp.UTC().Format(sas.TimeFormat)),
	}

	var udc, err = a.client.GetUserDelegationCredential(context.Background(), keyInfo, nil)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"storageAccount": a.storageAccount,
		"tenant":         a.tenantID,
		"start":          *keyInfo.Start,
		"expiry":         *keyInfo.Expiry,
	}).Info("refreshed Azure Storage User Delegation Credential")

	a.udc.exp = exp
	a.udc.inner = udc

	return a.udc.inner, nil
}
