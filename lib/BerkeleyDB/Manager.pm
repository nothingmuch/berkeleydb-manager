#!/usr/bin/perl

package BerkeleyDB::Manager;
use Moose;

use Carp qw(croak);

use BerkeleyDB;

use namespace::clean -except => 'meta';

our $VERSION = "0.02";

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

	delete($self->open_dbs->{$name})->db_close;
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
		my ( $txn, @dbs ) = @$d;

		$self->close_db($_) for @dbs;

		return $txn;
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

	$self->_pop_transaction;

    if ( $success ) {
        return wantarray ? @result : $result[0];
    } else {
		my $rollback_exception = do {
			local $@;
			eval { $self->txn_rollback($txn) };
			$@;
		};

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
