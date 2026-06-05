package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/geodata"
	_ "github.com/metacubex/mihomo/component/geodata/standard"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/resource"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/oschwald/maxminddb-golang"
	"golang.org/x/sync/errgroup"
)

var (
	geoUpdateConfigValue = atomic.NewTypedValue(geoUpdateConfig{})
	geoUpdateRunner      = geoUpdaterRunner{}

	updatingGeo atomic.Bool
)

var geoUpdatedHook struct {
	sync.RWMutex
	fn func()
}

var geoUpdateRetryBackoffForRunner = geoUpdateRetryBackoff

type geoUpdateConfig struct {
	autoUpdate     bool
	updateInterval int
}

type geoUpdaterRunner struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	config geoUpdateConfig
}

func GeoAutoUpdate() bool {
	return geoUpdateConfigValue.Load().autoUpdate
}

func GeoUpdateInterval() int {
	return geoUpdateConfigValue.Load().updateInterval
}

func SetGeoAutoUpdate(newAutoUpdate bool) {
	configureGeoUpdater(geoUpdateConfig{
		autoUpdate:     newAutoUpdate,
		updateInterval: GeoUpdateInterval(),
	})
}

func SetGeoUpdateInterval(newGeoUpdateInterval int) {
	configureGeoUpdater(geoUpdateConfig{
		autoUpdate:     GeoAutoUpdate(),
		updateInterval: newGeoUpdateInterval,
	})
}

func ConfigureGeoUpdater(newAutoUpdate bool, newGeoUpdateInterval int) {
	configureGeoUpdater(geoUpdateConfig{
		autoUpdate:     newAutoUpdate,
		updateInterval: newGeoUpdateInterval,
	})
}

func SetGeoUpdatedHook(fn func()) {
	geoUpdatedHook.Lock()
	defer geoUpdatedHook.Unlock()
	geoUpdatedHook.fn = fn
}

func notifyGeoUpdated() {
	geoUpdatedHook.RLock()
	fn := geoUpdatedHook.fn
	geoUpdatedHook.RUnlock()
	if fn != nil {
		fn()
	}
}

func configureGeoUpdater(config geoUpdateConfig) {
	geoUpdateConfigValue.Store(config)

	geoUpdateRunner.mu.Lock()
	defer geoUpdateRunner.mu.Unlock()

	if geoUpdateRunner.cancel != nil && geoUpdateRunner.config == config {
		return
	}
	if geoUpdateRunner.cancel != nil {
		geoUpdateRunner.cancel()
		geoUpdateRunner.cancel = nil
	}
	geoUpdateRunner.config = config

	if !config.autoUpdate {
		return
	}
	if config.updateInterval <= 0 {
		log.Errorln("[GEO] Invalid update interval: %d", config.updateInterval)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	geoUpdateRunner.cancel = cancel
	go runGeoUpdater(ctx, config.updateInterval)
}

func UpdateMMDB() (err error) {
	vehicle := resource.NewHTTPVehicle(geodata.MmdbUrl(), C.Path.MMDB(), "", nil, defaultHttpTimeout, 0)
	var oldHash utils.HashType
	if buf, err := os.ReadFile(vehicle.Path()); err == nil {
		oldHash = utils.MakeHash(buf)
	}
	data, hash, err := vehicle.Read(context.Background(), oldHash)
	if err != nil {
		return fmt.Errorf("can't download MMDB database file: %w", err)
	}
	if oldHash.Equal(hash) { // same hash, ignored
		return nil
	}
	if len(data) == 0 {
		return fmt.Errorf("can't download MMDB database file: no data")
	}

	instance, err := maxminddb.FromBytes(data)
	if err != nil {
		return fmt.Errorf("invalid MMDB database file: %s", err)
	}
	_ = instance.Close()

	defer mmdb.ReloadIP()
	mmdb.IPInstance().Reader.Close() //  mmdb is loaded with mmap, so it needs to be closed before overwriting the file
	if err = vehicle.Write(data); err != nil {
		return fmt.Errorf("can't save MMDB database file: %w", err)
	}
	return nil
}

func UpdateASN() (err error) {
	vehicle := resource.NewHTTPVehicle(geodata.ASNUrl(), C.Path.ASN(), "", nil, defaultHttpTimeout, 0)
	var oldHash utils.HashType
	if buf, err := os.ReadFile(vehicle.Path()); err == nil {
		oldHash = utils.MakeHash(buf)
	}
	data, hash, err := vehicle.Read(context.Background(), oldHash)
	if err != nil {
		return fmt.Errorf("can't download ASN database file: %w", err)
	}
	if oldHash.Equal(hash) { // same hash, ignored
		return nil
	}
	if len(data) == 0 {
		return fmt.Errorf("can't download ASN database file: no data")
	}

	instance, err := maxminddb.FromBytes(data)
	if err != nil {
		return fmt.Errorf("invalid ASN database file: %s", err)
	}
	_ = instance.Close()

	defer mmdb.ReloadASN()
	mmdb.ASNInstance().Reader.Close() //  mmdb is loaded with mmap, so it needs to be closed before overwriting the file
	if err = vehicle.Write(data); err != nil {
		return fmt.Errorf("can't save ASN database file: %w", err)
	}
	return nil
}

func UpdateGeoIp() (err error) {
	geoLoader, err := geodata.GetGeoDataLoader("standard")

	vehicle := resource.NewHTTPVehicle(geodata.GeoIpUrl(), C.Path.GeoIP(), "", nil, defaultHttpTimeout, 0)
	var oldHash utils.HashType
	if buf, err := os.ReadFile(vehicle.Path()); err == nil {
		oldHash = utils.MakeHash(buf)
	}
	data, hash, err := vehicle.Read(context.Background(), oldHash)
	if err != nil {
		return fmt.Errorf("can't download GeoIP database file: %w", err)
	}
	if oldHash.Equal(hash) { // same hash, ignored
		return nil
	}
	if len(data) == 0 {
		return fmt.Errorf("can't download GeoIP database file: no data")
	}

	if _, err = geoLoader.LoadIPByBytes(data, "cn"); err != nil {
		return fmt.Errorf("invalid GeoIP database file: %s", err)
	}

	defer geodata.ClearGeoIPCache()
	if err = vehicle.Write(data); err != nil {
		return fmt.Errorf("can't save GeoIP database file: %w", err)
	}
	return nil
}

func UpdateGeoSite() (err error) {
	geoLoader, err := geodata.GetGeoDataLoader("standard")

	vehicle := resource.NewHTTPVehicle(geodata.GeoSiteUrl(), C.Path.GeoSite(), "", nil, defaultHttpTimeout, 0)
	var oldHash utils.HashType
	if buf, err := os.ReadFile(vehicle.Path()); err == nil {
		oldHash = utils.MakeHash(buf)
	}
	data, hash, err := vehicle.Read(context.Background(), oldHash)
	if err != nil {
		return fmt.Errorf("can't download GeoSite database file: %w", err)
	}
	if oldHash.Equal(hash) { // same hash, ignored
		return nil
	}
	if len(data) == 0 {
		return fmt.Errorf("can't download GeoSite database file: no data")
	}

	if _, err = geoLoader.LoadSiteByBytes(data, "cn"); err != nil {
		return fmt.Errorf("invalid GeoSite database file: %s", err)
	}

	defer geodata.ClearGeoSiteCache()
	if err = vehicle.Write(data); err != nil {
		return fmt.Errorf("can't save GeoSite database file: %w", err)
	}
	return nil
}

func updateGeoDatabases() error {
	defer runtime.GC()

	b := errgroup.Group{}

	if geodata.GeoIpEnable() {
		if geodata.GeodataMode() {
			b.Go(UpdateGeoIp)
		} else {
			b.Go(UpdateMMDB)
		}
	}

	if geodata.ASNEnable() {
		b.Go(UpdateASN)
	}

	if geodata.GeoSiteEnable() {
		b.Go(UpdateGeoSite)
	}

	return b.Wait()
}

var ErrGetDatabaseUpdateSkip = errors.New("GEO database is updating, skip")

var updateGeoDatabasesForRunner = UpdateGeoDatabases

func UpdateGeoDatabases() error {
	log.Infoln("[GEO] Start updating GEO database")

	if updatingGeo.Load() {
		return ErrGetDatabaseUpdateSkip
	}

	updatingGeo.Store(true)
	defer updatingGeo.Store(false)

	log.Infoln("[GEO] Updating GEO database")

	if err := updateGeoDatabases(); err != nil {
		log.Errorln("[GEO] update GEO database error: %s", err.Error())
		return err
	}

	notifyGeoUpdated()

	return nil
}

func geoUpdateRetryBackoff(attempt int) time.Duration {
	retryIntervals := []time.Duration{
		10 * time.Second,
		30 * time.Second,
		time.Minute,
		5 * time.Minute,
		10 * time.Minute,
		30 * time.Minute,
	}
	if attempt < len(retryIntervals) {
		return retryIntervals[attempt]
	}
	return retryIntervals[len(retryIntervals)-1]
}

func updateGeoDatabasesWithRetry(ctx context.Context) bool {
	for attempt := 0; ; attempt++ {
		if err := updateGeoDatabasesForRunner(); err != nil {
			log.Errorln("[GEO] Failed to update GEO database: %s", err.Error())
			retryAfter := geoUpdateRetryBackoffForRunner(attempt)
			log.Warnln("[GEO] Retry GEO database update after %s", retryAfter)
			timer := time.NewTimer(retryAfter)
			select {
			case <-ctx.Done():
				timer.Stop()
				return false
			case <-timer.C:
				continue
			}
		}
		return true
	}
}

func getUpdateTime() (time time.Time, err error) {
	filesToCheck := []string{
		C.Path.GeoIP(),
		C.Path.MMDB(),
		C.Path.ASN(),
		C.Path.GeoSite(),
	}

	for _, file := range filesToCheck {
		var fileInfo os.FileInfo
		fileInfo, err = os.Stat(file)
		if err == nil {
			return fileInfo.ModTime(), nil
		}
	}

	return
}

func RegisterGeoUpdater() {
	configureGeoUpdater(geoUpdateConfigValue.Load())
}

func runGeoUpdater(ctx context.Context, updateInterval int) {
	interval := time.Duration(updateInterval) * time.Hour

	lastUpdate, err := getUpdateTime()
	if err != nil {
		log.Errorln("[GEO] Get GEO database update time error: %s", err.Error())
		return
	}

	log.Infoln("[GEO] last update time %s", lastUpdate)
	if lastUpdate.Add(interval).Before(time.Now()) {
		log.Infoln("[GEO] Database has not been updated for %v, update now", interval)
		if !updateGeoDatabasesWithRetry(ctx) {
			return
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Infoln("[GEO] updating database every %d hours", updateInterval)
			if !updateGeoDatabasesWithRetry(ctx) {
				return
			}
		}
	}
}
