package page

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/jungnoh/mora/common"
	"github.com/pkg/errors"
)

type PageHeader struct {
	LastTxId     uint64
	MarketCode   string
	Year         uint16
	CandleLength uint32
	Count        uint32
	StartOffset  uint32
	EndOffset    uint32
	Code         string
	Index        PageIndex
}

func ReadPageHeader(r io.Reader) (PageHeader, error) {
	header := PageHeader{}
	headerBin := make([]byte, HEADER_SIZE)
	if n, err := r.Read(headerBin); n < HEADER_SIZE || err != nil {
		return header, err
	}

	if !bytes.Equal(headerBin[0:4], []byte{0x20, 0x18, 0x10, 0x29}) {
		return header, errors.New("invalid page: magic byte incorrect")
	}
	if binary.LittleEndian.Uint16(headerBin[4:6]) != 1 {
		return header, errors.New("version invalid")
	}
	header.Year = binary.LittleEndian.Uint16(headerBin[6:8])
	header.CandleLength = binary.LittleEndian.Uint32(headerBin[8:12])
	header.Count = binary.LittleEndian.Uint32(headerBin[12:16])
	header.StartOffset = binary.LittleEndian.Uint32(headerBin[16:20])
	header.EndOffset = binary.LittleEndian.Uint32(headerBin[20:24])
	header.LastTxId = binary.LittleEndian.Uint64(headerBin[24:32])
	header.MarketCode = string(headerBin[32:42])
	header.Code = string(headerBin[42:60])
	header.Index = make([]uint32, INDEX_COUNT)

	indexBin := make([]byte, INDEX_ROW_COUNT*BLOCK_WIDTH)
	n, err := r.Read(indexBin)
	if n < len(indexBin) {
		return header, io.EOF
	}
	if err != nil {
		return header, err
	}
	for i := 0; i < INDEX_COUNT; i++ {
		header.Index[i] = binary.LittleEndian.Uint32(indexBin[4*i : 4*i+4])
	}
	return header, nil
}

func (p PageHeader) Write(w io.Writer) (err error) {
	encodedMarketCode := []byte(p.MarketCode)
	encodedCode := []byte(p.Code)
	if len(encodedMarketCode) > MAX_MARKET_CODE_LENGTH {
		err = errors.Errorf("code is too long (maximum %d, got %d)", MAX_MARKET_CODE_LENGTH, len(encodedMarketCode))
		return
	}
	if len(encodedCode) > MAX_CODE_LENGTH {
		err = errors.Errorf("code is too long (maximum %d, got %d)", MAX_CODE_LENGTH, len(encodedCode))
		return
	}
	if len(p.Index) > INDEX_COUNT {
		err = errors.Errorf("index array is too long (maximum %d, got %d)", INDEX_COUNT, len(p.Index))
		return
	}

	if _, err = w.Write([]byte{0x20, 0x18, 0x10, 0x29}); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, uint16(1)); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.Year); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.CandleLength); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.Count); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.StartOffset); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.EndOffset); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.LastTxId); err != nil {
		return
	}
	if _, err = w.Write(encodedMarketCode); err != nil {
		return
	}
	if _, err = w.Write(make([]byte, MAX_MARKET_CODE_LENGTH-len(encodedMarketCode))); err != nil {
		return
	}
	if _, err = w.Write(encodedCode); err != nil {
		return
	}
	if _, err = w.Write(make([]byte, MAX_CODE_LENGTH-len(encodedCode))); err != nil {
		return
	}
	for i := 0; i < INDEX_COUNT; i++ {
		dataToWrite := p.Count
		if i < len(p.Index) {
			dataToWrite = p.Index[i]
		}
		if err = binary.Write(w, binary.LittleEndian, dataToWrite); err != nil {
			return
		}
	}
	return
}

// Utility methods
func (h PageHeader) TimestampInPageRange(ts int64) bool {
	start := common.GetStartOfYearTimestamp(int(h.Year))
	end := common.GetStartOfYearTimestamp(int(h.Year) + 1)
	return start <= ts && ts < end
}

func (h PageHeader) CalculateTimestampOffset(ts int64) (offset uint32, inRange bool) {
	inRange = h.TimestampInPageRange(ts)
	offset = uint32(ts - common.GetStartOfYearTimestamp(int(h.Year)))
	return
}

func (h PageHeader) GetFirstTime() time.Time {
	return time.Unix(h.GetFirstTimestamp(), 0)
}

func (h PageHeader) GetFirstTimestamp() int64 {
	return int64(h.StartOffset) + common.GetStartOfYearTimestamp(int(h.Year))
}

func (h PageHeader) GetLastTime() time.Time {
	return time.Unix(h.GetLastTimestamp(), 0)
}

func (h PageHeader) GetLastTimestamp() int64 {
	return int64(h.EndOffset) + common.GetStartOfYearTimestamp(int(h.Year))
}
