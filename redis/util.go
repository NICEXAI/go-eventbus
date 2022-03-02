package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"regexp"
	"strconv"
	"strings"
)

func version(ctx context.Context, client *redis.Client) string {
	reg := regexp.MustCompile(`redis_version:([0-9,.]+)`)
	info := client.Do(ctx, "info", "server").String()
	if list := reg.FindStringSubmatch(info); len(list) == 2 {
		return list[1]
	}
	return ""
}

func isSupportStream(ctx context.Context, client *redis.Client) bool {
	ver := version(ctx, client)
	if len(ver) > 0 {
		vList := strings.Split(ver, ".")
		if vNum, err := strconv.Atoi(vList[0]); err == nil && vNum > 4 {
			return true
		}
	}
	return false
}
