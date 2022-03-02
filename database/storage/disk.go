package storage

import (
	"github.com/jungnoh/mora/page"
)

func (s *Storage) processDiskLoads() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case req := <-s.diskLoadChan:
			req.Response <- s.processDiskLoad(&req)
		}
	}
}
func (s *Storage) processDiskStores() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case req := <-s.diskStoreChan:
			req.Response <- s.processDiskStore(&req)
		}
	}
}

func (s *Storage) processDiskLoad(req *diskLoadRequest) diskLoadResponse {
	if req.HeaderOnly {
		result, err := s.disk.ReadHeader(req.Key)
		if err != nil {
			return diskLoadResponse{
				Error:      err,
				HeaderOnly: true,
				Exists:     false,
			}
		}
		return diskLoadResponse{
			Error:      nil,
			HeaderOnly: true,
			Exists:     !result.IsZero(),
			Content: page.Page{
				Header: result,
			},
		}
	} else {
		result, err := s.disk.Read(req.Key)
		if err != nil {
			return diskLoadResponse{
				Error:      err,
				HeaderOnly: false,
				Exists:     false,
			}
		}
		return diskLoadResponse{
			Error:      nil,
			HeaderOnly: false,
			Exists:     !result.IsZero(),
			Content:    result,
		}
	}
}

func (s *Storage) processDiskStore(req *diskStoreRequest) diskStoreResponse {
	err := s.disk.Write(*req.Content)
	return diskStoreResponse{
		Error: err,
	}
}

func (s *Storage) diskLoad(set page.CandleSet) (content page.Page, exists bool, err error) {
	responseChan := make(chan diskLoadResponse)
	defer close(responseChan)
	s.diskLoadChan <- diskLoadRequest{
		Key:        set,
		Response:   responseChan,
		HeaderOnly: false,
	}
	result := <-responseChan
	if result.Error != nil {
		exists = false
		err = result.Error
		return
	}
	content = result.Content
	exists = result.Exists
	return
}

func (s *Storage) diskLoadHeader(set page.CandleSet) (header page.PageHeader, exists bool, err error) {
	responseChan := make(chan diskLoadResponse)
	defer close(responseChan)
	s.diskLoadChan <- diskLoadRequest{
		Key:        set,
		Response:   responseChan,
		HeaderOnly: true,
	}
	result := <-responseChan
	if result.Error != nil {
		exists = false
		err = result.Error
		return
	}
	header = result.Content.Header
	exists = result.Exists
	return
}

func (s *Storage) diskStore(set page.CandleSet, content *page.Page) error {
	responseChan := make(chan diskStoreResponse)
	defer close(responseChan)
	s.diskStoreChan <- diskStoreRequest{
		Key:      set,
		Content:  content,
		Response: responseChan,
	}
	result := <-responseChan
	return result.Error
}
