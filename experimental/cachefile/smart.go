package cachefile

import (
	"os"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
)

func (c *CacheFile) LoadSmartRouting(tag string) *adapter.SmartRoutingStats {
	var state adapter.SmartRoutingStats
	err := c.view(func(tx *bbolt.Tx) error {
		bucket := c.bucket(tx, bucketSmartRouting)
		if bucket == nil {
			return os.ErrNotExist
		}
		content := bucket.Get([]byte(tag))
		if len(content) == 0 {
			return os.ErrInvalid
		}
		return state.UnmarshalBinary(content)
	})
	if err != nil {
		return nil
	}
	return &state
}

func (c *CacheFile) StoreSmartRouting(tag string, state *adapter.SmartRoutingStats) error {
	return c.batch(func(tx *bbolt.Tx) error {
		bucket, err := c.createBucket(tx, bucketSmartRouting)
		if err != nil {
			return err
		}
		content, err := state.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(tag), content)
	})
}
