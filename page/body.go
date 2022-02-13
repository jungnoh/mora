package page

import (
	"encoding/binary"
	"io"

	"github.com/jungnoh/mora/common"
)

type PageBodyBlock struct {
	Timestamp       uint64
	TimestampOffset uint32

	BitFields uint32
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

func NewPageBodyBlock(year uint16, candle common.Candle) PageBodyBlock {
	ts := uint64(candle.Timestamp.Unix())
	return PageBodyBlock{
		Timestamp:       ts,
		TimestampOffset: uint32(int64(ts) - common.GetStartOfYearTimestamp(int(year))),
		BitFields:       candle.BitFields,
		Open:            candle.Open,
		High:            candle.High,
		Low:             candle.Low,
		Close:           candle.Close,
		Volume:          candle.Volume,
	}
}

func (p *PageBodyBlock) Read(_ uint32, r io.Reader) error {
	blockBin := make([]byte, BLOCK_WIDTH)
	n, err := r.Read(blockBin)
	if n < BLOCK_WIDTH {
		return io.EOF
	}
	if err != nil {
		return err
	}

	p.TimestampOffset = binary.LittleEndian.Uint32(blockBin[0:4])
	p.BitFields = binary.BigEndian.Uint32(blockBin[4:8])
	p.Open = common.Float64frombytes(blockBin[8:16])
	p.High = common.Float64frombytes(blockBin[16:24])
	p.Low = common.Float64frombytes(blockBin[24:32])
	p.Close = common.Float64frombytes(blockBin[32:40])
	p.Volume = common.Float64frombytes(blockBin[40:48])

	return nil
}

func (p *PageBodyBlock) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.LittleEndian, p.TimestampOffset); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, p.BitFields); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.Open); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.High); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.Low); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.Close); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, p.Volume); err != nil {
		return
	}
	return
}

func (p *PageBodyBlock) SetYear(year uint16) {
	p.Timestamp = uint64(common.GetStartOfYearTimestamp(int(year))) + uint64(p.TimestampOffset)
}
