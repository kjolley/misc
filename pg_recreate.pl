#!/usr/bin/env perl
#Drop/recreate PostgreSQL databases from backup
#Written by Keith Jolley
#Copyright (c) 2015-2023, University of Oxford
#E-mail: keith.jolley@zoo.ox.ac.uk
#
#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with BIGSdb.  If not, see <http://www.gnu.org/licenses/>.
#Version 20231018
use strict;
use warnings;
use 5.010;
###########Local configuration#####################################################################
use constant {
	USER       => 'postgres',
	PASSWORD   => undef,                                     #Better to set password in .pgpass file
	BACKUP_DIR => '/mnt/nfs/filestore2/backups/databases',
	BINARIES   => '/usr/bin',
	EXCLUDE    => 'template0,template1'
};
#######End Local configuration#####################################################################
use Getopt::Long qw(:config no_ignore_case);
use DBI;
use Term::Cap;
use POSIX;
my %opts;
GetOptions(
	'd|directory=s' => \$opts{'d'},
	'h|help'        => \$opts{'h'},
	'l|list_only'   => \$opts{'l'},
	'v|vacuum'      => \$opts{'v'},
	'x|exclude=s'   => \$opts{'x'}
) or die("Error in command line arguments\n");
$opts{'d'} //= 'latest';
if ( $opts{'h'} ) {
	show_help();
	exit;
}
main();

sub main {
	config_check();
	my $backup_list = get_backup_list();
	if ( $opts{'l'} ) {
		say $_ foreach @$backup_list;
		exit;
	}
	my $existing = get_existing_dbases();
	my %existing_dbases = map { $_ => 1 } @$existing;
	foreach my $dbase (@$backup_list) {
		my $bin = BINARIES;
		if ( $existing_dbases{$dbase} ) {
			say "Dropping database $dbase...";
			system "$bin/dropdb -U postgres $dbase";
		}
		say "Creating database $dbase from dump...";
		system "$bin/createdb -U postgres $dbase";
		my $threads = $opts{'t'} // 1;
		my $dir = get_backup_dir();
		if (-e "$dir/$dbase.gz"){
			system "$bin/gunzip -c $dir/$dbase.gz | psql $dbase > /dev/null";
		} elsif (-d "$dir/$dbase" && glob("$dir/$dbase/*.gz")){
			system "$bin/zcat $dir/$dbase/*.gz | psql $dbase > /dev/null"
		}
		if ( $opts{'v'} ) {
			say "Vacuum analyzing database $dbase...";
			system "$bin/psql -c 'VACUUM ANALYZE' $dbase > /dev/null";
		}
	}
}
exit;

sub get_backup_dir {
	my $dir = BACKUP_DIR;
	$dir .= '/' if $dir !~ /\/$/;
	$dir .= ( $opts{'d'} // '' );
	return $dir;
}

sub get_backup_list {
	my $dir = get_backup_dir();
	die "Directory $dir does not exist.\n" if !-e $dir;
	$opts{'x'} //= q();
	my @exclude = split /,/x, $opts{'x'};
	my %exclude;
	foreach my $db (@exclude) {
		$db =~ s/\s//gx;
		$exclude{$db} = 1;
	}
	opendir( DIR, $dir ) or die $!;
	my @dbases;
	while ( my $file = readdir(DIR) ) {
		next if $file =~ /^\./;
		if ( $file =~ /([A-z0-9_\-]+)\.gz$/ ) {
			next if $exclude{$1};
			push @dbases, $1;
		}
		
		if (-d "$dir/$file" && glob("$dir/$file/*.gz")){
			next if $exclude{$file};
			push @dbases,$file;
		}
	}
	@dbases = sort @dbases;
	return \@dbases;
}

sub get_existing_dbases {
	my $db = DBI->connect( 'DBI:Pg:dbname=template1', USER, PASSWORD );
	my $list = $db->selectcol_arrayref("SELECT datname FROM pg_database");
	$db->disconnect;
	my %hash_list = map { $_ => 1 } @$list;
	my @always_exclude = split /,/, EXCLUDE;
	delete $hash_list{$_} foreach @always_exclude;
	my @filtered_list = sort keys %hash_list;
	return \@filtered_list;
}

sub config_check {
	my @binaries = qw(pg_dump vacuumdb pigz);
	my $fail     = 0;
	foreach my $binary (@binaries) {
		my $full_path = BINARIES . "/$binary";
		if ( !-e $full_path ) {
			say "$full_path does not exist.";
			$fail = 1;
		} elsif ( !-x $full_path ) {
			say "$full_path is not executable.";
			$fail = 1;
		}
	}
	exit if $fail;
}

sub show_help {
	my $termios = POSIX::Termios->new;
	$termios->getattr;
	my $ospeed = $termios->getospeed;
	my $t = Tgetent Term::Cap { TERM => undef, OSPEED => $ospeed };
	my ( $norm, $bold, $under ) = map { $t->Tputs( $_, 1 ) } qw/me md us/;
	say << "HELP";
${bold}NAME$norm
    ${bold}pg_recreate.pl$norm - Drop/recreate PostgreSQL databases

${bold}SYNOPSIS$norm
    ${bold}pg_recreate.pl$norm [${under}options$norm]

${bold}OPTIONS$norm
${bold}-d, --directory$norm ${under}DIRECTORY$norm
    Directory to recover from - this is relative to the BACKUP_DIR.
    Default 'latest'.    
   
${bold}-h, --help$norm
    This help page.
    
${bold}-l, --list_only$norm
    List databases that would be recreated.
          
${bold}-v, --vacuum$norm
    Run vacuum analyze on database after recover.
    
${bold}-x, --exclude$norm ${under}DATABASES$norm
    Comma-separated list of databases to exclude.
    
HELP
	return;
}
