#!/usr/bin/perl

package BerkeleyDB::Manager;
use Moose;

use Carp qw(croak);

use BerkeleyDB;

use Scope::Guard;
use Data::Stream::Bulk::Util qw(nil);
use Data::Stream::Bulk::Callback;
use Data::Stream::Bulk::Array;

use namespace::clean -except => 'meta';

our $VERSION = "0.03";

has open_dbs => (
	isa => "HashRef",
	is  => "ro",
	default => sub { +{} },
);

has [qw(dup dupsort)] => ( # read_uncomitted log_autoremove multiversion
	isa => "Bool",
	is  => "ro",
);

has [qw(autocommit transactions recover create)] => ( # snapshot, sync
	isa => "Bool",
	is  => "ro",
	default => 1,
);

has home => (
    is  => "ro",
	predicate => "has_home",
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

has env => (
    isa => "BerkeleyDB::Env",
    is  => "ro",
    lazy_build => 1,
);

sub _build_env {
    my $self = shift;

    BerkeleyDB::Env->new(
        ( $self->has_home ? ( -Home => $self->home ) : () ),
        -Flags => $self->env_flags,
    ) || die $BerkeleyDB::Error;
}

sub build_db_flags {
	my ( $self, %args ) = @_;

	if ( $self->has_db_flags ) {
		return $self->db_flags;
	}

	foreach my $opt ( qw(autocommit create) ) {
		$args{$opt} = $self->$opt unless exists $args{$opt};
	}

	my $flags = 0;

	if ( $args{autocommit} and $self->env_flags & DB_INIT_TXN && !$self->_current_transaction ) {
		$flags |= DB_AUTO_COMMIT;
	}

	if ( $args{create} ) {
		$flags |= DB_CREATE;
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
    ) || $BerkeleyDB::Error;
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
    my ( $self, $coderef ) = ( shift, shift );

    ref $coderef eq 'CODE' or croak '$coderef must be a CODE reference';

	my $txn = $self->txn_begin( $self->_current_transaction );

	$self->_push_transaction($txn);

    my @result;

    my ( $success, $err ) = do {
        local $@;

        my $success = eval {
            if ( wantarray ) {
                @result = $coderef->(@_);
            } elsif( defined wantarray ) {
                $result[0] = $coderef->(@_);
            } else {
                $coderef->(@_);
            }

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
			eval { $self->txn_rollback($txn) };
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

    my $txn = $self->env->TxnMgr->txn_begin($parent_txn || ()) || die $BerkeleyDB::Error;

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

sub dup_cursor_to_stream {
	my ( $self, @args ) = @_;

	my %args = @args;

	my ( $init, $key, $first, $cb, $cursor, $db, $n ) = delete @args{qw(init key callback_first callback cursor db chunk_size)};

	$key ||= '';

	$cursor ||= ( $db || croak "either 'cursor' or 'db' is a required argument" )->db_cursor;

	$first ||= sub {
		my ( $c, $r ) = @_;
		my $v;

		my $ret;

		if ( ( $ret = $c->c_get($key, $v, DB_SET) ) == 0 ) {
			push(@$r, [ $key, $v ]);
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
			push(@$r, [ $key, $v ]);
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
			my $rest = $self->cursor_to_stream(@args, callback => $cb, cursor => $cursor);
			return $bulk->cat($rest);
		}

		return $bulk;
	} else {
		return nil();
	}
}

sub cursor_to_stream {
    my ( $self, %args ) = @_;

	my ( $init, $cb, $cursor, $db, $f, $n ) = delete @args{qw(init callback cursor db flag chunk_size)};

	$cursor ||= ( $db || croak "either 'cursor' or 'db' is a required argument" )->db_cursor;

	$f ||= DB_NEXT;

	$cb ||= do {
		my ( $k, $v ) = ( '', '' );

		sub {
			my ( $c, $r ) = @_;

			if ( $c->c_get($k, $v, $f) == 0 ) {
				push(@$r, [ $k, $v ]);
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
true.

=item transactions

Whether or not to enable transactions.

Defaults to true.

=item autocommit

Whether or not a top level transaction is automatically created by BerkeleyDB.
Defaults to true.

If you turn this off note that all database handles must be opened inside a
transaction, unless transactions are disabled.

=item recover

If true (the default) C<DB_REGISTER> and C<DB_RECOVER> are enabled in the flags
to the env.

This will enable automatic recovery in case of a crash.

=item dup

Enables C<DB_DUP> in C<-Properties> by default, allowing duplicate keys in the db.

=item dupsort

Enables C<DB_DUPSORT> in C<-Properties> by default.

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

See C<cursor_to_stream>.

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

=item cursor_to_stream %args

Fetches data from a cursor, returning a L<Data::Stream::Bulk>.

If C<cursor> is not provided but C<db> is, a new cursor will be created.

If C<callback> is provided it will be invoked on the cursor with an accumilator
array repeatedly until it returns a false value. For example, to extract
triplets from a secondary index, you can use this callback:

	my ( $sk, $pk, $v ) = ( '', '', '' ); # to avoid uninitialized warnings from BDB

	$m->cursor_to_stream(
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

Lastly, C<init> is an optional callback that is invoked once before each chunk,
that can be used to set up the database. The return value is retained until the
chunk is finished, so this callback can return a L<Scope::Guard> to perform
cleanup.

=item dup_cursor_to_stream %args

A specialization of C<cursor_to_stream> for fetching duplicate key entries.

Takes the same arguments as C<cursor_to_stream>, but adds a few more.

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