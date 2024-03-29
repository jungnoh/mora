package command

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/concurrency"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

const insertCommandHeadSize uint32 = 38

type InsertCommand struct {
	Year         uint16
	CandleLength uint32
	MarketCode   string
	Code         string
	Count        uint32
	Candles      []common.TimestampCandle
}

func NewInsertCommand(set page.CandleSet, candles common.TimestampCandleList) InsertCommand {
	return InsertCommand{
		Year:         set.Year,
		CandleLength: set.CandleLength,
		MarketCode:   set.MarketCode,
		Code:         set.Code,
		Count:        uint32(len(candles)),
		Candles:      candles,
	}
}

func (e *InsertCommand) Read(size uint32, r io.Reader) error {
	if size < insertCommandHeadSize || (size-insertCommandHeadSize)%48 != 0 {
		return errors.New("wrong data size")
	}
	headerBin := make([]byte, insertCommandHeadSize)
	n, err := r.Read(headerBin)
	if uint32(n) < insertCommandHeadSize {
		return io.EOF
	}
	if err != nil {
		return err
	}

	e.Year = binary.LittleEndian.Uint16(headerBin[0:2])
	e.CandleLength = binary.LittleEndian.Uint32(headerBin[2:6])
	e.MarketCode = common.ReadNullPaddedString(headerBin[6:16])
	e.Code = common.ReadNullPaddedString(headerBin[16:34])
	e.Count = binary.LittleEndian.Uint32(headerBin[34:38])
	e.Candles = make([]common.TimestampCandle, e.Count)
	for i := uint32(0); i < e.Count; i++ {
		if err := e.Candles[i].Read(48, r); err != nil {
			return err
		}
	}
	return nil
}

func (e *InsertCommand) Write(w io.Writer) (err error) {
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

func (e *InsertCommand) BinarySize() uint32 {
	return insertCommandHeadSize + uint32(48*len(e.Candles))
}

func (e *InsertCommand) TypeId() CommandType {
	return InsertCommandType
}

func (e *InsertCommand) Plan() CommandPlan {
	return CommandPlan{
		NeededLocks: []NeededLock{
			{
				Lock:      concurrency.NewSetResourceName(e.targetSet()),
				Exclusive: true,
			},
		},
	}
}

func (e *InsertCommand) Execute(accessor PageSetAccessor) (interface{}, error) {
	pageKey := e.targetSet().UniqueKey()
	unlock, err := accessor.AcquirePage(e.targetSet(), true)
	if err != nil {
		return struct{}{}, errors.Wrapf(err, "InsertCommand: acquire failed (key '%s')", pageKey)
	}
	defer unlock()

	page, err := accessor.GetPage(e.targetSet(), true)
	if err != nil {
		return struct{}{}, errors.Wrapf(err, "InsertCommand: page load failed (key '%s')", pageKey)
	}
	if err := page.Add(common.TimestampCandleList(e.Candles).ToCandleList()); err != nil {
		return struct{}{}, errors.Wrapf(err, "InsertCommand: failed (key '%s')", pageKey)
	}
	return struct{}{}, nil
}

func (e *InsertCommand) targetSet() page.CandleSet {
	return page.CandleSet{
		Year: e.Year,
		CandleSetWithoutYear: page.CandleSetWithoutYear{
			CandleLength: e.CandleLength,
			MarketCode:   e.MarketCode,
			Code:         e.Code,
		},
	}
}

func (e *InsertCommand) String() string {
	return fmt.Sprintf("INSERT(%s,%s,%d,%d)", e.MarketCode, e.Code, e.CandleLength, e.Year)
}
