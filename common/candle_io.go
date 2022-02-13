package common

import (
	"encoding/binary"
	"io"
)

func (t *TimestampCandle) Write(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, t.Timestamp); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, t.BitFields); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, t.Open); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, t.High); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, t.Low); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, t.Close); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, t.Volume); err != nil {
		return err
	}
	return nil
}

func (t *TimestampCandle) Read(_ uint32, r io.Reader) error {
	bin := make([]byte, 48)
	n, err := r.Read(bin)
	if uint32(n) < 48 {
		return io.EOF
	}
	if err != nil {
		return err
	}
	t.Timestamp = int64(binary.LittleEndian.Uint32(bin[0:8]))
	t.TimelessCandle = TimelessCandle{
		BitFields: binary.BigEndian.Uint32(bin[8:12]),
		Open:      Float64frombytes(bin[12:16]),
		High:      Float64frombytes(bin[16:24]),
		Low:       Float64frombytes(bin[24:32]),
		Close:     Float64frombytes(bin[32:40]),
		Volume:    Float64frombytes(bin[40:48]),
	}
	return nil
}
