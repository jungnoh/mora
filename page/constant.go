package page

const BLOCK_WIDTH int = 48
const INDEX_ROW_COUNT int = 31
const HEADER_SIZE int = 60
const DATA_OFFSET int64 = int64(BLOCK_WIDTH*INDEX_ROW_COUNT + HEADER_SIZE)

// 12 columns, 31 rows of uint32
const INDEX_COUNT int = INDEX_ROW_COUNT * (BLOCK_WIDTH / 4)
const MAX_MARKET_CODE_LENGTH int = 10
const MAX_CODE_LENGTH int = 18
