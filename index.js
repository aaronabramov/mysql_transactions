/*

CREATE DATABASE test;

USE test;

ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password'

flush privileges;


*/

const mysql = require('mysql');
const chalk = require('chalk');
const stripAnsi = require('strip-ansi');
const indentString = require('indent-string');
const dateformat = require('dateformat');
const leftPad = require('left-pad');

const WIDTH = 100;
const PREFIX = chalk.red(`| `);
const SUFFIX = chalk.red(' |');
const PREFIX_LEN = stripAnsi(PREFIX).length;
const SUFFIX_LEN = stripAnsi(SUFFIX).length;

const ISOLATION = {
    REPEATABLE_READ: "REPEATABLE READ",
    READ_COMMITTED: "READ COMMITTED",
    READ_UNCOMMITTED: "READ UNCOMMITTED",
    SERIALIZABLE: "SERIALIZABLE",
}

const sleep = time => new Promise(r => setTimeout(r, time));

const date = d => {
    const date = d ? new Date(Date.parse(d)) : new Date();
    return dateformat(d, "yyyy-mm-dd HH:MM:ss");;;
};

const log = (str, indent_level) => {
    console.log(wrap(str, indent_level));
}

const wrap = (s, indent_level) => {
    const ts = (new Date()).toLocaleTimeString('en-US');
    const lines = s.split(/\n/);
    const stack = lines.reverse();
    const result = [];


    while (stack.length) {
        const next_line = stack.pop();

        if ((next_line.length + ts.length) <= WIDTH) {
            const fill = new Array(WIDTH - stripAnsi(next_line).length - PREFIX_LEN - SUFFIX_LEN).fill(" ").join("");
            const line = PREFIX + ` ${chalk.dim('[' + ts + ']')} ${chalk.yellow(next_line)}` + fill + SUFFIX;
            result.push(line);
        } else {
            const fit = next_line.substr(0, WIDTH - ts.length);
            const rest = next_line.substr(WIDTH - ts.length);
            stack.push(rest);
            stack.push(fit);
        }
    }
    return indentString(result.join('\n'), WIDTH * indent_level - indent_level + ts.length * indent_level + indent_level * SUFFIX_LEN + indent_level * PREFIX_LEN);
}

class Conn {
    log(s) {
        log(s, this.indent_level);
    }
    constructor(mysql_conn, indent_level) {
        this.mysql_conn = mysql_conn;
        this.indent_level = indent_level;
        this.log_result = false;
    }

    query(q, ...rest) {
        this.log(q);
        if (rest.length) {
            rest.forEach(r => this.log(JSON.stringify(r)));
        }
        return new Promise((res, rej) => {
            this.mysql_conn.query(q, ...rest, (error, results, fields) => {
                if (error) {
                    return rej(error);
                }

                let result_log = 'DONE';
                if (this.log_result) {
                    if (fields) {
                        const padding = 12;
                        const names = fields.map(f => f.name);
                        const result = ['| ' + names.map(n => n.padEnd(padding)).join(' | ')];
                        results.forEach(row => {
                            result.push(
                                '| ' + names.map(n => (String(row[n]) || 'e').padEnd(padding)).join(' | ')
                            );

                        });

                        result_log += "\n" + result.join('\n');
                    }
                }
                this.log(result_log + '\n');
                return res([results, fields]);
            })
        })
    }


    begin() {
        this.log('BEGIN');
        return new Promise((resolve, reject) => {
            this.mysql_conn.beginTransaction(err => {
                if (err) {
                    this.log('BEGIN ERROR' + err);
                    return reject(err);
                }
                resolve();
            })
        });
    }

    commit() {
        this.log('COMMIT');
        return new Promise((resolve, reject) => {
            this.mysql_conn.commit(err => {
                if (err) {
                    this.log('COMMIT ERROR' + err);
                    return reject(err);
                }
                resolve();
            })
        });
    }

    rollback() {
        return new Promise((resolve, reject) => {
            this.log('ROLLBACK');
            this.mysql_conn.rollback(err => {
                if (err) {
                    this.log('ROLLBACK ERROR' + err);
                    return reject(err);
                }
                resolve();
            })
        });
    }

}

const getConn = (indent_level) => {
    const mysql_conn = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: 'password',
        database: 'test'
    });

    return new Conn(mysql_conn, indent_level);
}

const run = async () => {
    let conn1 = getConn(0);
    let conn2 = getConn(1);

    conn1.log(chalk.blue(new Array(50).fill('*').join('')));
    conn1.log(chalk.blue("CONNECTION 1"));
    conn1.log(chalk.blue(new Array(50).fill('*').join('')));

    conn2.log(chalk.blue(new Array(50).fill('*').join('')));
    conn2.log(chalk.blue("CONNECTION 2"));
    conn2.log(chalk.blue(new Array(50).fill('*').join('')));


    const isolation = ISOLATION.REPEATABLE_READ;
    // await conn1.query("SET autocommit = 0;");
    await conn1.query(`SET TRANSACTION ISOLATION LEVEL ${isolation};`);
    // await conn2.query("SET autocommit = 0;");
    await conn2.query(`SET TRANSACTION ISOLATION LEVEL ${isolation};`);

    await conn1.query('DROP TABLE IF EXISTS test_table;');
    await conn1.query(`
    CREATE TABLE test_table (
       name varchar(256) NOT NULL,
       type varchar(256) NOT NULL,
       version bigint(20) NOT NULL,
       id bigint(20) NOT NULL,
       string_blob varchar(4096) NOT NULL,
       year bigint(20) NOT NULL,
       PRIMARY KEY(name, type, version, id),
       KEY year(year) USING BTREE
    );`);



    await conn1.begin();
    await conn1.query(`
    INSERT INTO test_table (name, type, version, id, string_blob, year)
    VALUES ?
    `, [[
        ['n1', 't1', 999, 1, "blob", 2019],
        ['n1', 't1', 999, 2, "blob", 2020],
        ['n1', 't1', 888, 3, "blob", 2020],
        ['n1', 't1', 999, 4, "blob", 2022],
    ]]);

    conn1.log_result = true;
    conn2.log_result = true;

    await conn2.query(`SELECT * FROM test_table`);
    await conn1.commit();

    await conn2.query(`SELECT * FROM test_table`);


    await conn1.begin();
    await conn1.query("SELECT * FROM test_table WHERE name = 'n1' AND version = 999 FOR UPDATE");
    // await conn1.query(`
    // REPLACE INTO test_table (name, type, version, id, string_blob, year)
    // VALUES ?
    // `, [[
    //     ['n1', 't1', 999, 1, "blob c1", 2019],
    // ]]);
    const deferred_commit = sleep(3000).then(() => conn1.commit());

    {
        // conn 2 transaction
        await conn2.begin();
        await conn2.query("SELECT * FROM test_table WHERE name = 'n2' AND version = 888 FOR UPDATE");
        await conn2.query('REPLACE INTO test_table (name, type, version, id, string_blob, year) VALUES ?',
            // SPLICE IN NEW VALUES
            [[
                ['n1', 't1', 888, 5, "blob c2", 2020],
                ['n1', 't1', 888, 6, "blob c2", 2019],
            ]]);
        await conn2.commit();
    }





    await deferred_commit;

    conn1.log_result = true;
    await conn1.query(`SELECT * FROM test_table`);
}

run().then(() => process.exit(0)).catch((e) => {
    console.error(e);
    process.exit(1);
});
