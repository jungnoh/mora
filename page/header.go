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

func (p *PageHeader) Read(size uint32, r io.Reader) error {
	headerBin := make([]byte, HEADER_SIZE)
	if n, err := r.Read(headerBin); n < HEADER_SIZE || err != nil {
		return err
	}

	if !bytes.Equal(headerBin[0:4], []byte{0x20, 0x18, 0x10, 0x29}) {
		return errors.New("invalid page: magic byte incorrect")
	}
	if binary.LittleEndian.Uint16(headerBin[4:6]) != 1 {
		return errors.New("version invalid")
	}
	p.Year = binary.LittleEndian.Uint16(headerBin[6:8])
	p.CandleLength = binary.LittleEndian.Uint32(headerBin[8:12])
	p.Count = binary.LittleEndian.Uint32(headerBin[12:16])
	p.StartOffset = binary.LittleEndian.Uint32(headerBin[16:20])
	p.EndOffset = binary.LittleEndian.Uint32(headerBin[20:24])
	p.LastTxId = binary.LittleEndian.Uint64(headerBin[24:32])
	p.MarketCode = common.ReadNullPaddedString(headerBin[32:42])
	p.Code = common.ReadNullPaddedString(headerBin[42:60])
	p.Index = make([]uint32, INDEX_COUNT)

	indexBin := make([]byte, INDEX_ROW_COUNT*BLOCK_WIDTH)
	n, err := r.Read(indexBin)
	if n < len(indexBin) {
		return io.EOF
	}
	if err != nil {
		return err
	}
	for i := 0; i < INDEX_COUNT; i++ {
		p.Index[i] = binary.LittleEndian.Uint32(indexBin[4*i : 4*i+4])
	}
	return nil
}

func (p *PageHeader) Write(w io.Writer) error {
	if len(p.Index) > INDEX_COUNT {
		return errors.Errorf("index array is too long (maximum %d, got %d)", INDEX_COUNT, len(p.Index))
	}

	if _, err := w.Write([]byte{0x20, 0x18, 0x10, 0x29}); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(1)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.Year); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.CandleLength); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.Count); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.StartOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.EndOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.LastTxId); err != nil {
		return err
	}
	if err := common.WriteNullPaddedString(MAX_MARKET_CODE_LENGTH, p.MarketCode, w); err != nil {
		return errors.Wrap(err, "failed to write market code")
	}
	if err := common.WriteNullPaddedString(MAX_CODE_LENGTH, p.Code, w); err != nil {
		return errors.Wrap(err, "failed to write code")
	}
	for i := 0; i < INDEX_COUNT; i++ {
		dataToWrite := p.Count
		if i < len(p.Index) {
			dataToWrite = p.Index[i]
		}
		if err := binary.Write(w, binary.LittleEndian, dataToWrite); err != nil {
			return err
		}
	}
	return nil
}

// Utility methods
func (p PageHeader) TimestampInPageRange(ts int64) bool {
	start := common.GetStartOfYearTimestamp(int(p.Year))
	end := common.GetStartOfYearTimestamp(int(p.Year) + 1)
	return start <= ts && ts < end
}

func (p PageHeader) CalculateTimestampOffset(ts int64) (offset uint32, inRange bool) {
	inRange = p.TimestampInPageRange(ts)
	offset = uint32(ts - common.GetStartOfYearTimestamp(int(p.Year)))
	return
}

func (p PageHeader) GetFirstTime() time.Time {
	return time.Unix(p.GetFirstTimestamp(), 0)
}

func (p PageHeader) GetFirstTimestamp() int64 {
	return int64(p.StartOffset) + common.GetStartOfYearTimestamp(int(p.Year))
}

func (p PageHeader) GetLastTime() time.Time {
	return time.Unix(p.GetLastTimestamp(), 0)
}

func (p PageHeader) GetLastTimestamp() int64 {
	return int64(p.EndOffset) + common.GetStartOfYearTimestamp(int(p.Year))
}
