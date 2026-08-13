package engine

import "github.com/Code0987/supercache/pkg/set"

func setContainsBlob(blob, item []byte) bool {
	s, err := set.DecodeSet(blob)
	if err != nil {
		return false
	}
	return s.Contains(item)
}

func setCardBlob(blob []byte) int {
	s, err := set.DecodeSet(blob)
	if err != nil {
		return 0
	}
	return s.Len()
}

func setMembersBlob(blob []byte) [][]byte {
	s, err := set.DecodeSet(blob)
	if err != nil {
		return nil
	}
	return s.Members()
}
