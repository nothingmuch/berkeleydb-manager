#!/usr/bin/perl

package
BerkeleyDB::Manager::Test;

use strict;
use warnings;

use Test::More;

use base qw(Exporter);

our @EXPORT = qw(sok);

sub sok ($;$) {
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	ok( $_[0] == 0, ( @_ > 1 ? $_[1] : () ) ) || diag("$BerkeleyDB::Error");
}

__PACKAGE__

__END__
