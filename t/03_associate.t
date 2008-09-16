#!/usr/bin/perl

use strict;
use warnings;

use Test::More 'no_plan';
use Test::TempDir;

use ok 'BerkeleyDB::Manager';

{
	isa_ok( my $m = BerkeleyDB::Manager->new( home => temp_root ), "BerkeleyDB::Manager" );

	isa_ok( $m->env, "BerkeleyDB::Env" );

	my $pri = $m->open_db("primary.db");
	my $sec = $m->open_db("secondary.db");

	$m->associate(
		primary => $pri,
		secondary => $sec,
		callback => sub { return $_[1] }
	);

	$pri->db_put( "foo", "bar" );

	my ( $pkey, $v );
	ok( $sec->db_pget( "bar", $pkey, $v ) == 0, "get on secondary" );

	is( $pkey, "foo", "pkey fetched" );
	is( $v, "bar", "value" );
}
