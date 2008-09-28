#!/usr/bin/perl -w

use strict;

use ExtUtils::MakeMaker;

WriteMakefile(
	NAME         => 'BerkeleyDB::Manager',
	VERSION_FROM => 'lib/BerkeleyDB/Manager.pm',
	INSTALLDIRS  => 'site',
	SIGN         => 1,
	PL_FILES     => { },
	PREREQ_PM    => {
		'Test::use::ok' => 0,
		'Test::TempDir' => 0,
		'Test::Exception' => 0,
		'Moose' => 0,
		'namespace::clean' => '0.08',
		'BerkeleyDB' => 0,
		'Data::Stream::Bulk' => 0,
		'Path::Class' => 0,
	},
);

