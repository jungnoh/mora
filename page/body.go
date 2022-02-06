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

func ReadPageBodyBlock(header PageHeader, r io.Reader) (PageBodyBlock, error) {
	blockBin := make([]byte, BLOCK_WIDTH)
	n, err := r.Read(blockBin)
	if n < BLOCK_WIDTH {
		return PageBodyBlock{}, io.EOF
	}
	if err != nil {
		return PageBodyBlock{}, err
	}

	block := PageBodyBlock{
		TimestampOffset: binary.LittleEndian.Uint32(blockBin[0:4]),
		BitFields:       binary.BigEndian.Uint32(blockBin[4:8]),
		Open:            common.Float64frombytes(blockBin[8:16]),
		High:            common.Float64frombytes(blockBin[16:24]),
		Low:             common.Float64frombytes(blockBin[24:32]),
		Close:           common.Float64frombytes(blockBin[32:40]),
		Volume:          common.Float64frombytes(blockBin[40:48]),
	}
	block.Timestamp = uint64(common.GetStartOfYearTimestamp(int(header.Year))) + uint64(block.TimestampOffset)
	return block, nil
}

func (p PageBodyBlock) Write(w io.Writer) (err error) {
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
