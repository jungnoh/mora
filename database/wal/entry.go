package wal

import (
	"encoding/binary"
	"io"

	"github.com/jungnoh/mora/common"
	"github.com/pkg/errors"
)

const walInsertEntryHeadSize uint32 = 38

type WalInsertEntry struct {
	Year         uint16
	CandleLength uint32
	MarketCode   string
	Code         string
	Count        uint32
	Candles      []common.TimestampCandle
}

func readTimestampCandle(r io.Reader) (common.TimestampCandle, error) {
	bin := make([]byte, 48)
	n, err := r.Read(bin)
	if uint32(n) < 48 {
		return common.TimestampCandle{}, io.EOF
	}
	if err != nil {
		return common.TimestampCandle{}, err
	}
	return common.TimestampCandle{
		Timestamp: int64(binary.LittleEndian.Uint32(bin[0:8])),
		TimelessCandle: common.TimelessCandle{
			BitFields: binary.BigEndian.Uint32(bin[8:12]),
			Open:      common.Float64frombytes(bin[12:16]),
			High:      common.Float64frombytes(bin[16:24]),
			Low:       common.Float64frombytes(bin[24:32]),
			Close:     common.Float64frombytes(bin[32:40]),
			Volume:    common.Float64frombytes(bin[40:48]),
		},
	}, nil
}

func readInsertEntry(dataSize uint32, r io.Reader) (WalInsertEntry, error) {
	if dataSize < walInsertEntryHeadSize || (dataSize-walInsertEntryHeadSize)%48 != 0 {
		return WalInsertEntry{}, errors.New("wrong data size")
	}
	headerBin := make([]byte, walInsertEntryHeadSize)
	n, err := r.Read(headerBin)
	if uint32(n) < walInsertEntryHeadSize {
		return WalInsertEntry{}, io.EOF
	}
	if err != nil {
		return WalInsertEntry{}, err
	}

	blockCount := (dataSize - walInsertEntryHeadSize) / 48
	entry := WalInsertEntry{
		Year:         binary.LittleEndian.Uint16(headerBin[0:2]),
		CandleLength: binary.LittleEndian.Uint32(headerBin[2:6]),
		MarketCode:   string(headerBin[6:16]),
		Code:         string(headerBin[16:34]),
		Count:        binary.LittleEndian.Uint32(headerBin[34:38]),
		Candles:      make([]common.TimestampCandle, blockCount),
	}
	for i := uint32(0); i < blockCount; i++ {
		candle, err := readTimestampCandle(r)
		if err != nil {
			return entry, nil
		}
		entry.Candles[i] = candle
	}

	return entry, nil
}
