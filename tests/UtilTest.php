<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(Util::class)]
final class UtilTest extends TestCase {

    #[DataProvider('sanitizeProvider')]
    public function testSanitize(string $sql, string $sanitized): void {
        $this->assertSame($sanitized, Util::attributes($sql)['db.query.text']);
    }

    public static function sanitizeProvider(): iterable {
        yield [
            "select *, 'abc' from user join order on user.id = order.user_id and user.active = 1 where order.ordernumber = '00001'",
            'select *, ? from user join order on user.id = order.user_id and user.active = ? where order.ordernumber = ?',
        ];
        yield [
            "update user set first_name = 'Jane', last_name = 'Doe' where id = 1",
            'update user set first_name = ?, last_name = ? where id = ?',
        ];
        yield [
            "select * from user where name = 'äöü' and user.active = 1",
            "select * from user where name = ? and user.active = ?"
        ];
    }

    #[DataProvider('operationProvider')]
    public function testOperationName(string $sql, ?string $operation): void {
        $this->assertSame($operation, Util::attributes($sql)['db.operation.name']);
    }

    public static function operationProvider(): iterable {
        yield [
            "select * from user",
            'SELECT',
        ];
        yield [
            "START TRANSACTION",
            'START TRANSACTION',
        ];
        yield [
            "COMMIT",
            'COMMIT',
        ];
        yield [
            "ROLLBACK",
            'ROLLBACK',
        ];
        yield [
            "START TRANSACTION; INSERT INTO user VALUES ('abc'); COMMIT",
            "INSERT",
        ];
    }

    #[DataProvider('querySummaryProvider')]
    public function testQuerySummary(string $sql, ?string $operation): void {
        $this->assertSame($operation, Util::attributes($sql)['db.query.summary']);
    }

    public static function querySummaryProvider(): iterable {
        yield [
            <<<'SQL'
                SELECT *
                FROM   wuser_table
                WHERE  username = ?
                SQL,
            'SELECT wuser_table',
        ];
        yield [
            <<<'SQL'
                INSERT INTO shipping_details
                            (order_id,
                            address)
                SELECT order_id,
                       address
                FROM   orders
                WHERE  order_id = ?
                SQL,
            'INSERT shipping_details SELECT orders',
        ];
        yield [
            <<<'SQL'
                SELECT *
                FROM   songs,
                       artists
                WHERE  songs.artist_id == artists.id
                SQL,
            'SELECT songs artists',
        ];
        yield [
            <<<'SQL'
                SELECT order_date
                FROM   (SELECT *
                        FROM   orders o
                               JOIN customers c
                                 ON o.customer_id = c.customer_id)
                SQL,
            'SELECT SELECT orders customers',
        ];
        yield [
            <<<'SQL'
                SELECT *
                FROM   "song list",
                       'artists'
                SQL,
            'SELECT "song list" \'artists\'',
        ];
    }
}
