package wal

import (
	"encoding/binary"
	"io"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
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

func (e *WalInsertEntry) Read(size uint32, r io.Reader) error {
	if size < walInsertEntryHeadSize || (size-walInsertEntryHeadSize)%48 != 0 {
		return errors.New("wrong data size")
	}
	headerBin := make([]byte, walInsertEntryHeadSize)
	n, err := r.Read(headerBin)
	if uint32(n) < walInsertEntryHeadSize {
		return io.EOF
	}
	if err != nil {
		return err
	}

	blockCount := (size - walInsertEntryHeadSize) / 48
	e.Year = binary.LittleEndian.Uint16(headerBin[0:2])
	e.CandleLength = binary.LittleEndian.Uint32(headerBin[2:6])
	e.MarketCode = string(headerBin[6:16])
	e.Code = string(headerBin[16:34])
	e.Count = binary.LittleEndian.Uint32(headerBin[34:38])
	e.Candles = make([]common.TimestampCandle, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		if err := e.Candles[i].Read(48, r); err != nil {
			return err
		}
	}
	return nil
}

func (e *WalInsertEntry) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.LittleEndian, e.Year); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, e.CandleLength); err != nil {
		return
	}
	if err := common.WriteNullPaddedString(page.MAX_MARKET_CODE_LENGTH, e.MarketCode, w); err != nil {
		return errors.Wrap(err, "failed to write market code")
	}
	if err := common.WriteNullPaddedString(page.MAX_CODE_LENGTH, e.Code, w); err != nil {
		return errors.Wrap(err, "failed to write code")
	}
	if err = binary.Write(w, binary.LittleEndian, e.Count); err != nil {
		return
	}
	for _, candle := range e.Candles {
		if err = candle.Write(w); err != nil {
			return
		}
	}

	return nil
}
