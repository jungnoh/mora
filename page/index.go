package page

type PageIndex []uint32

func (p PageIndex) ApplyDailyCount(dailyCount PageIndex) {
	culSum := uint32(0)
	for i := 1; i < INDEX_COUNT; i++ {
		culSum += dailyCount[i-1]
		p[i] += culSum
	}
}
