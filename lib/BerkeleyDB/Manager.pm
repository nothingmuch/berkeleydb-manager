#!/usr/bin/perl

package BerkeleyDB::Manager;
use Moose;

use Carp qw(croak);

use BerkeleyDB;

use Data::Stream::Bulk::Util qw(nil);
use Data::Stream::Bulk::Callback;
use Data::Stream::Bulk::Array;

use Path::Class;

use Moose::Util::TypeConstraints;

use namespace::clean -except => 'meta';

our $VERSION = "0.09";

coerce( __PACKAGE__,
	from HashRef => via { __PACKAGE__->new(%$_) },
);

has open_dbs => (
	isa => "HashRef",
	is  => "ro",
	default => sub { +{} },
);

has [qw(
	dup
	dupsort
	recover
	create
	truncate
	multiversion
	read_uncomitted
	readonly
	log_auto_remove
)] => (
	isa => "Bool",
	is  => "ro",
);

has [qw(
	autocommit
	transactions
	snapshot
	sync
)] => (
	isa => "Bool",
	is  => "ro",
	default => 1,
);

has home => (
    is  => "ro",
	predicate => "has_home",
);

has log_dir => (
	is => "ro",
	predicate => "has_log_dir",
);

has temp_dir => (
	is => "ro",
	predicate => "has_temp_dir",
);

has data_dir => (
	is => "ro",
	predicate => "has_data_dir",
);

has db_class => (
	isa => "ClassName",
	is  => "ro",
	default => "BerkeleyDB::Btree",
);

has env_flags => (
	isa => "Int",
	is  => "ro",
	lazy_build => 1,
);

has db_properties => (
	isa => "Int",
	is  => "ro",
	predicate => "has_db_properties",
);

has db_flags => (
	isa => "Int",
	is  => "ro",
	predicate => "has_db_flags",
);

has chunk_size => (
	isa => "Int",
	is  => "rw",
	default => 500,
);

sub _build_env_flags {
	my $self = shift;

	my $flags = DB_INIT_MPOOL;

	if ( $self->transactions ) {
		$flags |= DB_INIT_TXN;

		if ( $self->recover ) {
			$flags |= DB_REGISTER | DB_RECOVER;
		}
	}

	if ( $self->create ) {
		$flags |= DB_CREATE;
	}

	return $flags;
}

has env_config => (
	isa => "HashRef",
	is  => "ro",
	lazy_build => 1,
);

sub _build_env_config {
	my $self = shift;

	return {
		( $self->has_log_dir  ? ( DB_LOG_DIR  => $self->log_dir  ) : () ),
		( $self->has_data_dir ? ( DB_DATA_DIR => $self->data_dir ) : () ),
		( $self->has_temp_dir ? ( DB_TEMP_DIR => $self->temp_dir ) : () ),
	};
}

has env => (
    isa => "BerkeleyDB::Env",
    is  => "ro",
    lazy_build => 1,
);

sub _build_env {
    my $self = shift;

	my $flags = $self->env_flags;

	if ( $self->create ) {
		my $home = $self->has_home ? dir($self->home) : dir();

		$home->mkpath unless -d $home;

		foreach my $name ( "log_dir", "data_dir", "temp_dir" ) {
			my $pred = "has_$name";
			next unless $self->$pred;

			my $dir = $home->subdir( $self->$name );

			$dir->mkpath unless -d $dir;
		}

		my @config;

		push @config, "set_lg_dir " . $self->log_dir if $self->has_log_dir;
		push @config, "set_data_dir " . $self->data_dir if $self->has_data_dir;
		push @config, "set_tmp_dir " . $self->temp_dir if $self->has_temp_dir;
		push @config, "db_log_autoremove" if $self->log_auto_remove;

		if ( $flags & DB_MULTIVERSION ) {
			push @config, "db_multiversion";
		}

		if ( @config ) {
			my $config = $home->file("DB_CONFIG");

			unless ( -e $config ) {
				my $fh = $config->openw;
				$fh->print("set_lg_dir " . $self->log_dir . "\n") if $self->has_log_dir; # set_lg_dir is not a typo
				$fh->print("set_data_dir " . $self->data_dir . "\n") if $self->has_data_dir;
			}
		}
	}

	my $env = BerkeleyDB::Env->new(
		( $self->has_home ? ( -Home => $self->home ) : () ),
		-Flags  => $flags,
		-Config => $self->env_config,
	) || die $BerkeleyDB::Error;

	if ( $self->log_auto_remove ) {
		$self->assert_version( 4.7, "log_auto_remove" );
		$env->log_set_config( DB_LOG_AUTO_REMOVE, 1 );
	}

	return $env;
}

sub build_db_flags {
	my ( $self, %args ) = @_;

	if ( exists $args{flags} ) {
		return $args{flags};
	}

	my $flags = 0;

	if ( $self->has_db_flags ) {
		$flags = $self->db_flags;
	} else {
		foreach my $opt ( qw(create readonly truncate) ) {
			$args{$opt} = $self->$opt unless exists $args{$opt};
		}

		unless ( exists $args{autocommit} ) {
			# if there is a current transaction the DB open and all subsequent
			# operations are already protected by it, an specifying auto commit
			# will fail
			# furthermore, specifying autocommit without having transactions makes
			# no sense
			$args{autocommit} = $self->autocommit && $self->env_flags & DB_INIT_TXN && !$self->_current_transaction;
		}
	}

	if ( exists $args{autocommit} ) {
		if ( $args{autocommit} ) {
			$flags |= DB_AUTO_COMMIT;
		} else {
			$flags &= ~DB_AUTO_COMMIT;
		}
	}

	if ( exists $args{create} ) {
		if ( $args{create} ) {
			$flags |= DB_CREATE;
		} else {
			$flags &= ~DB_CREATE;
		}
	}

	if ( exists $args{truncate} ) {
		if ( $args{truncate} ) {
			$flags |= DB_TRUNCATE;
		} else {
			$flags &= ~DB_TRUNCATE;
		}
	}

	if ( exists $args{readonly} ) {
		if ( $args{readonly} ) {
			$flags |= DB_RDONLY;
		} else {
			$flags &= ~DB_RDONLY;
		}
	}

	if ( exists $args{multiversion} ) {
		if ( $args{multiversion} ) {
			$flags |= DB_MULTIVERSION;
		} else {
			$flags &= ~DB_MULTIVERSION;
		}
	}

	if ( exists $args{read_uncomitted} ) {
		if ( $args{read_uncomitted} ) {
			$flags |= DB_READ_UNCOMMITTED;
		} else {
			$flags &= ~DB_READ_UNCOMMITTED;
		}
	}

	return $flags;
}

sub build_db_properties {
	my ( $self, %args ) = @_;

	if ( $self->has_db_properties ) {
		return $self->db_properties;
	}

	foreach my $opt ( qw(dup dupsort) ) {
		$args{$opt} = $self->$opt unless exists $args{$opt};
	}

	my $props = 0;

	if ( $args{dup} ) {
		$props |= DB_DUP;

		if ( $args{dupsort} ) {
			$props |= DB_DUPSORT;
		}
	}

	return $props;
}

sub instantiate_btree {
	my ( $self, @args ) = @_;

	$self->instantiate_db( @args, class => "BerkeleyDB::Btree" );
}

sub instantiate_hash {
	my ( $self, @args ) = @_;

	$self->instantiate_db( @args, class => "BerkeleyDB::Hash" );
}

sub instantiate_db {
    my ( $self, %args ) = @_;

	my $class = $args{class} || $self->db_class;
	my $file  = $args{file}  || croak "no 'file' arguemnt provided";

	my $flags = $args{flags}      || $self->build_db_flags(%args);
	my $props = $args{properties} || $self->build_db_properties(%args);

	my $txn   = $args{txn} || ( $self->env_flags & DB_INIT_TXN && $self->_current_transaction );

	$class->new(
		-Filename => $file,
		-Env      => $self->env,
		( $txn   ? ( -Txn      => $txn   ) : () ),
		( $flags ? ( -Flags    => $flags ) : () ),
		( $props ? ( -Property => $props ) : () ),
    ) || die $BerkeleyDB::Error;
}

sub get_db {
	my ( $self, $name ) = @_;

	$self->open_dbs->{$name};
}

sub open_db {
	my ( $self, @args ) = @_;

	unshift @args, "file" if @args % 2 == 1;

	my %args = @args;

    my $name = $args{name} || $args{file} || croak "no 'name' or 'file' arguemnt provided";

	if ( my $db = $self->get_db($name) ) {
		return $db;
	} else {
		return $self->register_db( $name, $self->instantiate_db(%args) );
	}
}

sub register_db {
	my ( $self, $name, $db ) = @_;

	if ( my $frame = $self->_transaction_stack->[-1] ) {
		push @$frame, $name;
	}

	$self->open_dbs->{$name} = $db;
}

sub close_db {
	my ( $self, $name ) = @_;

	if ( my $db = delete($self->open_dbs->{$name}) ) {
		if ( $db->db_close != 0 ) {
			die $BerkeleyDB::Error;
		}
	}

	return 1;
}

sub all_open_dbs {
	my $self = shift;
	values %{ $self->open_dbs };
}

sub associate {
	my ( $self, %args ) = @_;

	$self->assert_version(4.6, "associate");

	my ( $primary, $secondary, $callback ) = @args{qw(primary secondary callback)};

	foreach my $db ( $primary, $secondary ) {
		unless ( ref $db ) {
			my $db_obj = $self->get_db($db) || die "no such db: $db";
			$db = $db_obj;
		}
	}

    if( $primary->associate( $secondary, sub {
		my ( $id, $val ) = @_;

		if ( defined ( my $value = $callback->($id, $val) ) ) {
			$_[2] = $value;
		}

		return 0;
    } ) != 0 ) {
		die $BerkeleyDB::Error;
    }
}

has _transaction_stack => (
	isa => "ArrayRef",
	is  => "ro",
	default => sub { [] },
);

sub _current_transaction {
	my $self = shift;

	if ( my $frame = $self->_transaction_stack->[-1] ) {
		return $frame->[0];
	}

	return;
}

sub _push_transaction {
	my ( $self, $txn ) = @_;
	push @{ $self->_transaction_stack }, [ $txn ];
}

sub _pop_transaction {
	my ( $self ) = @_;

	if ( my $d = pop @{ $self->_transaction_stack } ) {
		shift @$d;
		$self->close_db($_) for @$d;
	} else {
		croak "Transaction stack underflowed";
	}
}

sub txn_do {
	my ( $self, $coderef, %args ) = @_;

	my @args = @{ $args{args} || [] };

	my ( $commit, $rollback ) = @args{qw(commit rollback)};

	ref $coderef eq 'CODE' or croak '$coderef must be a CODE reference';

	my $txn = $self->txn_begin( $self->_current_transaction );

	$self->_push_transaction($txn);

	my @result;

	my $wantarray = wantarray; # gotta capture, eval { } has its own

	my ( $success, $err ) = do {
		local $@;

		my $success = eval {
			if ( $wantarray ) {
				@result = $coderef->(@args);
			} elsif( defined $wantarray ) {
				$result[0] = $coderef->(@args);
			} else {
				$coderef->(@args);
			}

			$commit && $commit->();
			$self->txn_commit($txn);

			1;
		};

		( $success, $@ );
	};

	if ( $success ) {
		undef $txn;
		$self->_pop_transaction;
		return wantarray ? @result : $result[0];
	} else {
		my $rollback_exception = do {
			local $@;
			eval { $self->txn_rollback($txn); $rollback && $rollback->() };
			$@;
		};

		undef $txn;
		$self->_pop_transaction;

		if ($rollback_exception) {
			croak "Transaction aborted: $err, rollback failed: $rollback_exception";
		}

		die $err;
	}
}

sub txn_begin {
	my ( $self, $parent_txn ) = @_;

	my $txn = $self->env->TxnMgr->txn_begin($parent_txn || undef, $self->multiversion && $self->snapshot ? DB_TXN_SNAPSHOT : 0 ) || die $BerkeleyDB::Error;

	$txn->Txn($self->all_open_dbs);

	return $txn;
}

sub txn_commit {
	my ( $self, $txn ) = @_;

	unless ( $txn->txn_commit == 0 ) {
		die $BerkeleyDB::Error;
	}

	return 1;
}

sub txn_rollback {
	my ( $self, $txn ) = @_;

	unless ( $txn->txn_abort == 0 ) {
		die $BerkeleyDB::Error;
	}

	return 1;
}

sub dup_cursor_stream {
	my ( $self, @args ) = @_;

	my %args = @args;

	my ( $init, $key, $first, $cb, $cursor, $db, $n ) = delete @args{qw(init key callback_first callback cursor db chunk_size)};

	my ( $values, $keys ) = @args{qw(values keys)};
	my $pairs = !$values && !$keys;
	croak "'values' and 'keys' are mutually exclusive" if $values && $keys;

	$key ||= '';

	$cursor ||= ( $db || croak "either 'cursor' or 'db' is a required argument" )->db_cursor;

	$first ||= sub {
		my ( $c, $r ) = @_;
		my $v;

		my $ret;

		if ( ( $ret = $c->c_get($key, $v, DB_SET) ) == 0 ) {
			push(@$r, $pairs ? [ $key, $v ] : ( $values ? $v : $key ));
		} elsif ( $ret == DB_NOTFOUND ) {
			return;
		} else {
			die $BerkeleyDB::Error;
		}

	};

	$cb ||= sub {
		my ( $c, $r ) = @_;
		my $v;
		my $ret;
		if ( ( $ret = $c->c_get($key, $v, DB_NEXT_DUP) ) == 0 ) {
			push(@$r, $pairs ? [ $key, $v ] : ( $values ? $v : $key ));
		} elsif ( $ret == DB_NOTFOUND ) {
			return;
		} else {
			die $BerkeleyDB::Error;
		}
	};

	$n ||= $self->chunk_size;

	my $g = $init && $self->$init(%args);

	my $ret = [];
	my $bulk = Data::Stream::Bulk::Array->new( array => $ret );

	if ( $cursor->$first($ret) ) {
		$cursor->c_count(my $count);

		if ( $count > 1 ) { # more entries for the same value

			# fetch up to $n times
			for ( 1 .. $n-1 ) {
				unless ( $cursor->$cb($ret) ) {
					return $bulk;
				}
			}

			# and defer the rest
			my $rest = $self->cursor_stream(@args, callback => $cb, cursor => $cursor);
			return $bulk->cat($rest);
		}

		return $bulk;
	} else {
		return nil();
	}
}

sub cursor_stream {
	my ( $self, %args ) = @_;

	my ( $init, $cb, $cursor, $db, $f, $n ) = delete @args{qw(init callback cursor db flag chunk_size)};

	my ( $values, $keys ) = @args{qw(values keys)};
	my $pairs = !$values && !$keys;
	croak "'values' and 'keys' are mutually exclusive" if $values && $keys;

	$cursor ||= ( $db || croak "either 'cursor' or 'db' is a required argument" )->db_cursor;

	$f ||= DB_NEXT;

	$cb ||= do {
		my ( $k, $v ) = ( '', '' );

		sub {
			my ( $c, $r ) = @_;

			if ( $c->c_get($k, $v, $f) == 0 ) {
				push(@$r, $pairs ? [ $k, $v ] : ( $values ? $v : $k ));
			} elsif ( $c->status == DB_NOTFOUND ) {
				return;
			} else {
				die $BerkeleyDB::Error;
			}
		}
	};

	$n ||= $self->chunk_size;

	Data::Stream::Bulk::Callback->new(
		callback => sub {
			return unless $cursor;

			my $g = $init && $self->$init(%args);

			my $ret = [];

			for ( 1 .. $n ) {
				unless ( $cursor->$cb($ret) ) {
					# we're done, this is the last block
					undef $cursor;
					return ( scalar(@$ret) && $ret );
				}
			}

			return $ret;
		},
	);
}

sub assert_version {
	my ( $self, $version, $feature ) = @_;

	unless ( $self->have_version($version) ) {
		croak "$feature requires DB $version, but we only have $BerkeleyDB::db_version";
	}
}

sub have_version {
	my ( $self, $version ) = @_;
	$BerkeleyDB::db_version >= $version;
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

=pod

=head1 NAME

BerkeleyDB::Manager - General purpose L<BerkeleyDB> wrapper

=head1 SYNOPSIS

	use BerkeleyDB::Manager;

	my $m = BerkeleyDB::Manager->new(
		home => Path::Class::Dir->new( ... ), # if you want to use rel paths
		db_class => "BerkeleyDB::Hash", # the default class for new DBs
	);

	my $db = $m->open_db( file => "foo" ); # defaults

	$m->txn_do(sub {
		$db->db_put("foo", "bar");
		die "error!"; # rolls back
	});

	# fetch all key/value pairs as a Data::Stream::Bulk
	my $pairs = $m->cursor_stream( db => $db );

=head1 DESCRIPTION

This object provides a convenience wrapper for L<BerkeleyDB>

=head1 ATTRIBUTES

=over 4

=item home

The path to pass as C<-Home> to C<< BerkeleyDB::Env->new >>.

If provided the C<file> arguments to C<open_db> should be relative paths.

If not provided, L<BerkeleyDB> will use the current working directory for
transaction journals, etc.

=item create

Whether C<DB_CREATE> is passed to C<Env> or C<instantiate_db> by default. Defaults to
false.

=item readonly

Whether C<DB_RDONLY> is passed in the flags. Defaults to false.

=item transactions

Whether or not to enable transactions.

Defaults to true.

=item autocommit

Whether or not a top level transaction is automatically created by BerkeleyDB.
Defaults to true.

If you turn this off note that all database handles must be opened inside a
transaction, unless transactions are disabled.

=item recover

If true C<DB_REGISTER> and C<DB_RECOVER> are enabled in the flags to the env.

This will enable automatic recovery in case of a crash.

See also the F<db_recover> utility, and
L<file:///usr/local/BerkeleyDB/docs/gsg_txn/C/architectrecovery.html#multiprocessrecovery>

=item multiversion

Enables multiversioning concurrency.

See
L<http://www.oracle.com/technology/documentation/berkeley-db/db/gsg_txn/C/isolation.html#snapshot_isolation>

=item snapshot

Whether or not C<DB_TXN_SNAPSHOT> should be passed to C<txn_begin>.

If C<multiversion> is not true, this is a noop.

Defaults to true.

Using C<DB_TXN_SNAPSHOT> means will cause copy on write multiversioning
concurrency instead of locking concurrency.

This can improve read responsiveness for applications with long running
transactions, by allowing a page to be read even if it is being written to in
another transaction since the writer is modifying its own copy of the page.

This is an alternative to enabling reading of uncomitted data, and provides the
same read performance while maintaining snapshot isolation at the cost of more
memory.

=item read_uncomitted

Enables uncomitted reads.

This breaks the I in ACID, since transactions are no longer isolated.

A better approaach to increase read performance when there are long running
writing transactions is to enable multiversioning.

=item log_auto_remove

Enables automatic removal of logs.

Normally logs should be removed after being backed up, but if you are not
interested in having full snapshot backups for catastrophic recovery scenarios,
you can enable this.

See L<http://www.oracle.com/technology/documentation/berkeley-db/db/ref/transapp/logfile.html>.

Defaults to false.

=item sync

Enables syncing of BDB log writing.

Defaults to true.

If disabled, transaction writing will not be synced. This means that in the
event of a crash some successfully comitted transactions might still be rolled
back during recovery, but the database will still be in tact and atomicity is
still guaranteed.

This is useful for bulk imports as it can significantly increase performance of
smaller transactions.

=item dup

Enables C<DB_DUP> in C<-Properties>, allowing duplicate keys in the db.

Defaults to false.

=item dupsort

Enables C<DB_DUPSORT> in C<-Properties>.

Defaults to false.

=item db_class

The default class to use when instantiating new DB objects. Defaults to
L<BerkeleyDB::Btree>.

=item env_flags

Flags to pass to the env. Overrides C<transactions>, C<create> and C<recover>.

=item db_flags

Flags to pass to C<instantiate_db>. Overrides C<create> and C<autocommit>.

=item db_properties

Properties to pass to C<instantiate_db>. Overrides C<dup> and C<dupsort>.

=item open_dbs

The hash of currently open dbs.

=item chunk_size

See C<cursor_stream>.

Defaults to 500.

=back

=head1 METHODS

=over 4

=item open_db %args

Fetch a database handle, opening it as necessary.

If C<name> is provided, it is used as the key in C<open_dbs>. Otherwise C<file>
is taken from C<%args>.

Calls C<instantiate_db>

=item close_db $name

Close the DB with the key C<$name>

=item get_db $name

Fetch the db specified by C<$name> if it is already open.

=item register_db $name, $handle

Registers the DB as open.

=item instantiate_db %args

Instantiates a new database handle.

C<file> is a required argument.

If C<class> is not provided, the L</db_class> will be used in place.

If C<txn> is not provided and the env has transactions enabled, the current
transaction if any is used. See C<txn_do>

C<flags> and C<properties> can be overridden manually. If they are not provided
C<build_db_flags> and C<build_db_properties> will be used.

=item instantiate_hash

=item instantiate_btree

Convenience wrappers for C<instantiate_db> that set C<class>.

=item build_db_properties %args

Merges argument options into a flag integer.

Default arguments are taken from the C<dup> and C<dupsort> attrs.

=item build_db_flags %args

Merges argument options into a flag integer.

Default arguments are taken from the C<autocommit> and C<create> attrs.

=item txn_do sub { }

Executes the subroutine in an C<eval> block. Calls C<txn_commit> if the
transaction was successful, or C<txn_rollback> if it wasn't.

Transactions are kept on a stack internally.

=item txn_begin $parent_txn

Begin a new transaction.

If C<$parent_txn> is provided the new transaction will be a child transaction.

The new transaction is set as the active transaction for all registered
database handles.

If C<multiversion> is enabled C<DB_TXN_SNAPSHOT> is passed in as well.

=item txn_commit $txn

Commit a transaction.

Will die on error.

=item txn_rollback $txn

Rollback a transaction.

=item associate %args

Associate C<secondary> with C<primary>, using C<callback> to extract keys.

C<callback> is invoked with the primary DB key and the value on every update to
C<primary>, and is expected to return a key (or with recent L<BerkeleyDB> also
an array reference of keys) with which to create indexed entries.

Fetching on C<secondary> with a secondary key returns the value from C<primary>.

Fetching with C<pb_get> will also return the primary key.

See the BDB documentation for more details.

=item all_open_dbs

Returns a list of all the registered databases.

=item cursor_stream %args

Fetches data from a cursor, returning a L<Data::Stream::Bulk>.

If C<cursor> is not provided but C<db> is, a new cursor will be created.

If C<callback> is provided it will be invoked on the cursor with an accumilator
array repeatedly until it returns a false value. For example, to extract
triplets from a secondary index, you can use this callback:

	my ( $sk, $pk, $v ) = ( '', '', '' ); # to avoid uninitialized warnings from BDB

	$m->cursor_stream(
		db => $db,
		callback => {
			my ( $cursor, $accumilator ) = @_;

			if ( $cursor->c_pget( $sk, $pk, $v ) == 0 ) {
				push @$accumilator, [ $sk, $pk, $v ];
				return 1;
			}

			return; # nothing left
		}
	);

If it is not provided, C<c_get> will be used, returning C<[ $key, $value ]> for
each cursor position. C<flag> can be passed, and defaults to C<DB_NEXT>.

C<chunk_size> controls the number of pairs returned in each chunk. If it isn't
provided the attribute C<chunk_size> is used instead.

If C<values> or C<keys> is set to a true value then only values or keys will be
returned. These two arguments are mutually exclusive.

Lastly, C<init> is an optional callback that is invoked once before each chunk,
that can be used to set up the database. The return value is retained until the
chunk is finished, so this callback can return a L<Scope::Guard> to perform
cleanup.

=item dup_cursor_stream %args

A specialization of C<cursor_stream> for fetching duplicate key entries.

Takes the same arguments as C<cursor_stream>, but adds a few more.

C<key> can be passed in to initialize the cursor with C<DB_SET>.

To do manual initialization C<callback_first> can be provided instead.

C<callback> is generated to use C<DB_NEXT_DUP> instead of C<DB_NEXT>, and
C<flag> is ignored.

=back

=head1 VERSION CONTROL

This module is maintained using Darcs. You can get the latest version from
L<http://nothingmuch.woobling.org/code>, and use C<darcs send> to commit
changes.

=head1 AUTHOR

Yuval Kogman E<lt>nothingmuch@woobling.orgE<gt>

=head1 COPYRIGHT

	Copyright (c) 2008 Yuval Kogman. All rights reserved
	This program is free software; you can redistribute
	it and/or modify it under the same terms as Perl itself.

=cut
