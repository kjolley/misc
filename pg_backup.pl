#!/usr/bin/perl
#Backup PostgreSQL databases using pigz (parallel gzip)
#Written by Keith Jolley
#Copyright (c) 2015, University of Oxford
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
use strict;
use warnings;
use 5.010;
###########Local configuration#####################################################################
use constant {
	USER           => 'postgres',
	PASSWORD       => undef,                                     #Better to set password in .pgpass file
	BACKUP_DIR     => '/mnt/nfs/filestore2/backups/databases',
	TMP_DIR        => '/var/tmp',
	BINARIES       => '/usr/bin',
	ALWAYS_EXCLUDE => 'template0,template1'
};
#######End Local configuration#####################################################################
use Getopt::Long qw(:config no_ignore_case);
use DBI;
use Term::Cap;
use POSIX;
my %opts;
GetOptions(
	'd|databases=s' => \$opts{'d'},
	'e|exclude=s'   => \$opts{'e'},
	'l|list_only'   => \$opts{'l'},
	'h|help'        => \$opts{'h'},
	't|threads=i'   => \$opts{'t'},
	'v|vacuum'      => \$opts{'v'},
	'w|weekly'      => \$opts{'w'}
) or die("Error in command line arguments\n");
my $EXIT = 0;
local @SIG{qw (INT TERM HUP)} = ( sub { $EXIT = 1 } ) x 3;    #Terminate on kill signal.

if ( $opts{'h'} ) {
	show_help();
	exit;
}
config_check();
my $list = get_database_list();
if ( $opts{'l'} ) {
	local $" = "\n";
	say "@$list";
	exit;
}
my $day      = get_day();
my $dest_dir = BACKUP_DIR;
$dest_dir .= "/$day" if $opts{'w'};
if ( !-d $dest_dir ) {
	eval { system( 'mkdir', '-p', $dest_dir ) };
	exit if $? >>= 8;
}
my $path = BINARIES;
foreach my $database (@$list) {
	last if $EXIT;
	my $status = "$database: ";
	print "$status\r";
	if ( $opts{'v'} ) {
		$status .= "vacuuming";
		print "$status\r";
		my $start = time;
		eval { system( "$path/vacuumdb", '-z', -U => USER, $database ) };
		exit if $? >>= 8;
		my $stop        = time;
		my $vacuum_time = $stop - $start;
		$status = "$database: vacuumed (${vacuum_time}s)";
		print "$status\r";
	}
	print "$status; dumping\r";
	my $start   = time;
	my $user    = USER;
	my $tmp_dir = TMP_DIR;
	$opts{'t'} //= 1;
	eval { system("$path/pg_dump -U $user $database | $path/pigz -p $opts{'t'} -c > '$tmp_dir/$database.gz'") };
	exit if $? >>= 8;
	my $stop      = time;
	my $dump_time = $stop - $start;
	$status .= "; dumped (${dump_time}s)";
	print "$status; moving\r";
	$start = time;
	eval( system("mv '$tmp_dir/$database.gz' '$dest_dir'") );
	exit if $? >>= 8;
	$stop = time;
	my $move_time = $stop - $start;
	$status .= "; moved (${move_time}s)";
	say "$status";
}

sub get_database_list {
	my $db = DBI->connect( 'DBI:Pg:dbname=template1', USER, PASSWORD );
	$list = $db->selectcol_arrayref("SELECT datname FROM pg_database ORDER BY datname");
	$db->disconnect;
	my %hash_list = map { $_ => 1 } @$list;
	if ( $opts{'d'} ) {
		my @list = split /,/, $opts{'d'};
		my @filtered_list;
		foreach (@list) {
			push @filtered_list, $_ if $hash_list{$_};
		}
		return \@filtered_list;
	}
	my @always_exclude = split /,/, ALWAYS_EXCLUDE;
	my @opt_exclude = $opts{'e'} ? split /,/, $opts{'e'} : ();
	delete $hash_list{$_} foreach ( @always_exclude, @opt_exclude );
	my @filtered_list = sort keys %hash_list;
	return \@filtered_list;
}

sub get_day {
	my %days = map { substr( $_, 0, 3 ) => $_ } qw/Monday Tuesday Wednesday Thursday Friday Saturday Sunday/;
	my $day = $days{ substr( localtime, 0, 3 ) };
	return $day;
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
    ${bold}pg_backup.pl$norm - Backup PostgreSQL databases

${bold}SYNOPSIS$norm
    ${bold}pg_backup.pl$norm [${under}options$norm]

${bold}OPTIONS$norm
${bold}-d, --databases$norm ${under}DATABASES$norm
    Comma-separated list of databases to backup.  Select all databases if
    unspecified.
    
${bold}-e, --exclude$norm ${under}DATABASES$norm
    Comma-separated list of databases to exclude from backup.  This is ignored
    if databases are explicitly listed using the --databases argument.
    
${bold}-l, --list_only$norm
    List databases that would be backed up.
    
${bold}-h, --help$norm
    This help page.
    
${bold}-t, --threads ${under}THREADS$norm
    Number of threads to use by pigz.  Default 1.
    
${bold}-v, --vacuum
    Run vacuum analyze on database before backup.  
    
${bold}-w, --weekly$norm
    Put backups in a directory named after the day (determined when the script
    begins).  This enables a weekly backup that overwrites the previous week's
    backups.

HELP
	return;
}
