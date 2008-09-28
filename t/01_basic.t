#!/usr/bin/perl

use strict;
use warnings;

use Test::More 'no_plan';
use Test::Exception;
use Test::TempDir qw(temp_root);

use Path::Class;

chdir temp_root(); # don't make a mess

use ok "BerkeleyDB::Manager";

{
	isa_ok( my $m = BerkeleyDB::Manager->new( create => 1 ), "BerkeleyDB::Manager" );

	isa_ok( $m->env, "BerkeleyDB::Env" );

	my $db;
	lives_ok { $db = $m->open_db( file => "foo.db" ) } "open with no home";

	isa_ok( $db, "BerkeleyDB::Btree" );

	is_deeply([ $m->all_open_dbs ], [ $db ], "open DBs" );
}

{
	isa_ok( my $m = BerkeleyDB::Manager->new( home => ".", create => 1 ), "BerkeleyDB::Manager" );

	isa_ok( $m->env, "BerkeleyDB::Env" );

	my $db;
	lives_ok { $db = $m->open_db( file => "foo.db" ) } "open with home";

	isa_ok( $db, "BerkeleyDB::Btree" );

	isa_ok( my $hash = $m->instantiate_hash( file => "hash" ), "BerkeleyDB::Hash" );

	is_deeply( $m->open_dbs, { "foo.db" => $db }, "hash db not registered" );

	$m->register_db( hash => $hash );

	is_deeply( $m->open_dbs, { "foo.db" => $db, hash => $hash }, "hash registered" );

	$m->close_db("hash");

	is_deeply( $m->open_dbs, { "foo.db" => $db }, "hash db closed" );
}

{
	isa_ok(
		my $m = BerkeleyDB::Manager->new(
			home => "subdirs",
			create => 1,
			log_dir  => "logs",
			data_dir => "data",
		),
		"BerkeleyDB::Manager"
	);

	isa_ok( $m->env, "BerkeleyDB::Env" );

	my $db;
	lives_ok { $db = $m->open_db( file => "stuff" ) } "open with log & data dirs";

	isa_ok( $db, "BerkeleyDB::Btree" );

	ok( -e file("subdirs", "data", "stuff"), "created under data dir" );
}

