#!/usr/bin/perl

use strict;
use warnings;

use Test::More 'no_plan';
use Test::Exception;
use Test::TempDir;

use ok "BerkeleyDB::Manager";

chdir temp_root(); # don't make a mess

{
	isa_ok( my $m = BerkeleyDB::Manager->new( home => "." ), "BerkeleyDB::Manager" );

	isa_ok( $m->env, "BerkeleyDB::Env" );

	my $db;
	lives_ok { $db = $m->open_db( file => "foo.db" ) } "open with no home";

	isa_ok( $db, "BerkeleyDB::Btree" );

	is_deeply([ $m->all_open_dbs ], [ $db ], "open DBs" );

	throws_ok {
		$m->txn_do(sub {
			ok( $db->db_get("foo", my $v) != 0, "get failed" );

			ok( $db->db_put("foo", "bar") == 0, "no error in put" );

			ok( $db->db_get("foo", $v) == 0, "no error in get" );
			is( $v, "bar", "'foo' key" );

			die "error";
		});
	} qr/error/, "dies in txn";

	{
		ok( $db->db_get("foo", my $v) != 0, "get failed (transaction aborted)" );
	}

	lives_ok {
		$m->txn_do(sub {
			ok( $db->db_get("foo", my $v) != 0, "get failed" );

			ok( $db->db_put("foo", "bar") == 0, "no error in put" );

			ok( $db->db_get("foo", $v) == 0, "no error in get" );
			is( $v, "bar", "'foo' key" );
		});
	} "no error in txn";

	{
		ok( $db->db_get("foo", my $v) == 0, "no error in get (transaction comitted)" );
		is( $v, "bar", "'foo' key" );
	}
}

{
	isa_ok( my $m = BerkeleyDB::Manager->new( home => "." ), "BerkeleyDB::Manager" );

	isa_ok( $m->env, "BerkeleyDB::Env" );

	my ( $first, $second ) = map { $m->open_db( file => $_ ) } qw(first.db second.db);

	is_deeply( [ sort $m->all_open_dbs ], [ sort $first, $second ], "open DBs" );

	throws_ok {
		$m->txn_do(sub {
			ok( $first->db_put("foo", "bar") == 0, "no error in put" );
			ok( $second->db_put("gorch", "zot") == 0, "no error in put" );

			die "error";
		});
	} qr/error/, "dies in txn";

	{
		ok( $first->db_get("foo", my $v) != 0, "get failed (transaction aborted)" );

		ok( $second->db_get("gorch", $v) != 0, "get failed (transaction aborted) in second db" );
	}

	lives_ok {
		$m->txn_do(sub {
			ok( $first->db_put("foo", "bar") == 0, "no error in put" );
			ok( $second->db_put("gorch", "zot") == 0, "no error in put" );
		});
	} "no error in txn";

	{
		ok( $first->db_get("foo", my $v) == 0, "get succeeded (transaction comitted)" );
		is( $v, "bar", "'foo' key" );

		ok( $second->db_get("gorch", $v) == 0, "get succeeded in second db" );
		is( $v, "zot", "'gorch' key" );
	}
}

{
	isa_ok( my $m = BerkeleyDB::Manager->new( home => "." ), "BerkeleyDB::Manager" );

	isa_ok( $m->env, "BerkeleyDB::Env" );

	my $db;
	lives_ok { $db = $m->open_db( file => "nested.db" ) } "open with no home";

	isa_ok( $db, "BerkeleyDB::Btree" );

	is_deeply([ $m->all_open_dbs ], [ $db ], "open DBs" );

	throws_ok {
		$m->txn_do(sub {
			ok( $db->db_get("foo", my $v) != 0, "get failed" );

			ok( $db->db_put("foo", "bar") == 0, "no error in put" );

			$m->txn_do(sub {
				ok( $db->db_get("gorch", my $v) != 0, "get failed" );

				ok( $db->db_put("gorch", "bar") == 0, "no error in put" );

				ok( $db->db_get("gorch", $v) == 0, "no error in get" );
				is( $v, "bar", "'gorch' key" );

				die "error";
			});
		})
	} qr/error/, "dies in inner txn";

	{
		ok( $db->db_get("foo", my $v) != 0, "get failed (transaction aborted)" );

		ok( $db->db_get("gorch", $v) != 0, "get failed (nested transaction aborted)" );
	}


	throws_ok {
		$m->txn_do(sub {
			ok( $db->db_get("foo", my $v) != 0, "get failed" );

			ok( $db->db_put("foo", "bar") == 0, "no error in put" );

			$m->txn_do(sub {
				ok( $db->db_get("gorch", my $v) != 0, "get failed" );

				ok( $db->db_put("gorch", "bar") == 0, "no error in put" );

				ok( $db->db_get("gorch", $v) == 0, "no error in get" );
				is( $v, "bar", "'gorch' key" );
			});

			die "error";
		})
	} qr/error/, "dies in outer txn";

	{
		ok( $db->db_get("foo", my $v) != 0, "get failed (transaction aborted)" );

		ok( $db->db_get("gorch", $v) != 0, "get failed (nested transaction aborted)" );
	}

	lives_ok {
		$m->txn_do(sub {
			ok( $db->db_get("foo", my $v) != 0, "get failed" );

			ok( $db->db_put("foo", "bar") == 0, "no error in put" );

			$m->txn_do(sub {
				ok( $db->db_get("gorch", my $v) != 0, "get failed" );

				ok( $db->db_put("gorch", "bar") == 0, "no error in put" );

				ok( $db->db_get("gorch", $v) == 0, "no error in get" );
				is( $v, "bar", "'gorch' key" );
			});
		});
	} "no error in txn";

	{
		ok( $db->db_get("foo", my $v) == 0, "no error in get (transaction comitted)" );
		is( $v, "bar", "'foo' key" );

		ok( $db->db_get("gorch", $v) == 0, "no error in get (transaction comitted)" );
		is( $v, "bar", "'foo' key" );
	}

	{
		ok( my $txn = $m->txn_begin, "parent transaction" );

			ok( $db->db_get("dancing", my $v) != 0, "get failed" );

			ok( $db->db_put("dancing", "bar") == 0, "no error in put" );

			ok( my $ctxn = $m->txn_begin($txn), "child transaction" );

				ok( $db->db_get("oi", $v) != 0, "get failed" );

				ok( $db->db_put("oi", "bar") == 0, "no error in put" );

				ok( $db->db_get("oi", $v) == 0, "no error in get" );
				is( $v, "bar", "'oi' key" );

			ok( $m->txn_rollback($ctxn), "rollback" );
			undef $ctxn;

			ok( $db->db_get("oi", $v) != 0, "get failed (rolled back)" );

			ok( $db->db_get("dancing", $v) == 0, "no error in get" );
			is( $v, "bar", "'dancing' key (only nested txn rolled back)" );

			ok( $ctxn = $m->txn_begin($txn), "child transaction" );

				ok( $db->db_get("oi", $v) != 0, "get failed" );

				ok( $db->db_put("oi", "hippies") == 0, "no error in put" );

				ok( $db->db_get("oi", $v) == 0, "no error in get" );
				is( $v, "hippies", "'oi' key" );

			ok( $m->txn_commit($ctxn), "commit" );

		ok( $m->txn_commit($txn), "commit" );


		ok( $db->db_get("dancing", $v) == 0, "no error in get" );
		is( $v, "bar", "'dancing' key" );

		ok( $db->db_get("oi", $v) == 0, "no error in get" );
		is( $v, "hippies", "'oi' key" );
	}
}
