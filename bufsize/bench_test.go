package bufsize

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgconn"
)

// prevent the compiler eliminating the function call.
// @see: https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go
var result [][]byte

func BenchmarkBufferSize(b *testing.B) {
	conn4KB, conn8KB, conn16KB, conn1MB := connect(4), connect(8), connect(16), connect(1024)
	ctx := context.Background()

	setupDB(conn4KB)

	b.Run("4KB", func(b *testing.B) {
		result = selectBench(b, ctx, conn4KB)
	})

	b.Run("8KB", func(b *testing.B) {
		result = selectBench(b, ctx, conn8KB)
	})

	b.Run("16KB", func(b *testing.B) {
		result = selectBench(b, ctx, conn16KB)
	})

	b.Run("1MB", func(b *testing.B) {
		result = selectBench(b, ctx, conn1MB)
	})
}

func selectBench(b *testing.B, ctx context.Context, conn *pgconn.PgConn) [][]byte {
	var r [][]byte
	for n := 0; n < b.N; n++ {
		r = selectRows(ctx, conn)
	}
	return r
}

func selectRows(ctx context.Context, conn *pgconn.PgConn) [][]byte {
	sql := `select * from t;`

	mrr := conn.Exec(ctx, sql)

	var bb [][]byte

	for mrr.NextResult() {
		rr := mrr.ResultReader()
		for rr.NextRow() {
			bb = rr.Values()
		}
	}

	return bb
}

func connect(KB int) *pgconn.PgConn {
	url := os.Getenv("PG_URL")
	if url == "" {
		panic("PG_URL is not defined")
	}

	config, err := pgconn.ParseConfig(url)
	if err != nil {
		panic(err)
	}

	config.MinReadBufferSize = KB * 1024

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		panic(err)
	}

	return conn
}

func setupDB(conn *pgconn.PgConn) {
	_, err := conn.Exec(context.Background(), `
		create table if not exists t as(
			select generate_series(1, 1000000) AS id, md5(random()::text) AS desc
		);`).ReadAll()
	if err != nil {
		panic(err)
	}
}
