package common

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *LogMeta) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "checkPointTs":
			z.CheckpointTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "CheckpointTs")
				return
			}
		case "resolvedTs":
			z.ResolvedTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ResolvedTs")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z LogMeta) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "checkPointTs"
	err = en.Append(0x82, 0xac, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.CheckpointTs)
	if err != nil {
		err = msgp.WrapError(err, "CheckpointTs")
		return
	}
	// write "resolvedTs"
	err = en.Append(0xaa, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ResolvedTs)
	if err != nil {
		err = msgp.WrapError(err, "ResolvedTs")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z LogMeta) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "checkPointTs"
	o = append(o, 0x82, 0xac, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x54, 0x73)
	o = msgp.AppendUint64(o, z.CheckpointTs)
	// string "resolvedTs"
	o = append(o, 0xaa, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x54, 0x73)
	o = msgp.AppendUint64(o, z.ResolvedTs)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *LogMeta) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "checkPointTs":
			z.CheckpointTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "CheckpointTs")
				return
			}
		case "resolvedTs":
			z.ResolvedTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ResolvedTs")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z LogMeta) Msgsize() (s int) {
	s = 1 + 13 + msgp.Uint64Size + 11 + msgp.Uint64Size
	return
}
