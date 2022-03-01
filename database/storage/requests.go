package storage

import "github.com/jungnoh/mora/page"

type diskStoreRequest struct {
	Key      page.CandleSet
	Content  *page.Page
	Response chan diskStoreResponse
}

type diskStoreResponse struct {
	Error error
}

type diskLoadRequest struct {
	Key      page.CandleSet
	Response chan diskLoadResponse
}

type diskLoadResponse struct {
	Error   error
	Exists  bool
	Content page.Page
}
