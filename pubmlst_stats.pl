#!/usr/bin/perl
#Generate summary stats for PubMLST databases using RESTful API
#Written by Keith Jolley
#Copyright (c) 2018, University of Oxford
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
use constant URI           => 'http://rest.pubmlst.org';
use constant CELL_MAXWIDTH => 30;
use constant TMP_DIR       => '/var/tmp';
use constant START_YEAR    => '2001';
use Getopt::Long qw(:config no_ignore_case);
use JSON;
use LWP::UserAgent;
use Parallel::ForkManager;
use Excel::Writer::XLSX;
use Term::Cap;
use POSIX;
my %ignore  = map { $_ => 1 } qw(pubmlst_rmlst_seqdef_kiosk pubmlst_plasmid_seqdef);
my %formats = map { $_ => 1 } qw(Excel text);
my %opts;
GetOptions(
	'dir=s'     => \$opts{'dir'},
	'format=s'  => \$opts{'format'},
	'help'      => \$opts{'help'},
	'threads=i' => \$opts{'threads'}
) or die("Error in command line arguments\n");
$opts{'threads'} //= 1;
$opts{'format'}  //= 'text';
$opts{'dir'}     //= '.';
$opts{'dir'} =~ s/\/$//x;
die "Invalid format.\n" if !$formats{ $opts{'format'} };

if ( $opts{'help'} ) {
	show_help();
	exit;
}
main();
exit;

sub main {
	my $taxa      = get_taxa_list();
	my $counts    = get_counts($taxa);
	my $countries = get_country_isolates($taxa);
	my $dates     = get_isolate_dates($taxa);
	my $excel     = initiate_excel();
	output_counts( $counts, $excel );
	output_countries( $countries, $excel );
	output_dates( $dates, $excel );

	#		use Data::Dumper;
	#		say Dumper $dates;
	return;
}

sub output_counts {
	my ( $counts, $excel ) = @_;
	my $text_out = "$opts{'dir'}/stats.txt";
	open( my $fh, '>:encoding(utf8)', $text_out ) || die "Cannot open $text_out for writing.\n";
	my @taxa = keys %$counts;
	say $fh qq(taxon\tisolates\tgenomes\tloci\talleles);
	foreach my $taxon ( sort @taxa ) {
		print $fh $taxon;
		print $fh qq(\t) . ( $counts->{$taxon}->{'isolates'} // q() );
		print $fh qq(\t) . ( $counts->{$taxon}->{'genomes'}  // q() );
		print $fh qq(\t) . ( $counts->{$taxon}->{'loci'}     // q() );
		say $fh qq(\t) . ( $counts->{$taxon}->{'sequences'} // q() );
	}
	close $fh;
	if ( $opts{'format'} eq 'Excel' ) {
		write_excel_tab( $excel, 'stats', $text_out );
		unlink $text_out;
	}
	return;
}

sub output_dates {
	my ( $dates, $excel ) = @_;
	my $text_out = "$opts{'dir'}/dates.txt";
	my @taxa     = sort keys %$dates;
	my ( $end_year, $end_month ) = get_end_dates();
	my @dates;
	foreach my $year ( START_YEAR .. $end_year ) {
		foreach my $month ( 1 .. 12 ) {
			push @dates, sprintf( '%d-%02d-01', $year, $month );
			last if $year == $end_year && $month == $end_month;
		}
	}
	open( my $fh, '>:encoding(utf8)', $text_out ) || die "Cannot open $text_out for writing.\n";
	local $" = qq(\t);
	say $fh qq(taxon\t@dates);
	foreach my $taxon ( sort keys %$dates ) {
		print $fh $taxon;
		foreach my $date (@dates) {
			print $fh qq(\t) . ( $dates->{$taxon}->{$date} );
		}
		print $fh qq(\n);
	}
	close $fh;
	if ( $opts{'format'} eq 'Excel' ) {
		write_excel_tab( $excel, 'isolates_added', $text_out );
		unlink $text_out;
	}
}

sub output_countries {
	my ( $countries, $excel ) = @_;
	my %country_list;
	my %ignore =
	  map { $_ => 1 } (
		'Arabian Peninsula',
		'Mixed source',
		'None',
		'Unknown',
		'Unknown: Africa',
		'Unknown - Africa',
		'Unknown - Asia',
		'Unknown - Europe',
		'Unknown - South America',
		'Yugoslavia'
	  );

	#Some databases have historical data - we need to map countries to current names
	my $country_map = { 'Czechoslavakia' => 'Czech Republic' };
	foreach my $taxon ( keys %$countries ) {
		foreach my $country ( keys %{ $countries->{$taxon} } ) {
			next if $ignore{$country};
			$country_list{$country} = 1;
		}
	}
	my @country_list = sort keys %country_list;
	my $text_out     = "$opts{'dir'}/isolate_countries.txt";
	local $" = qq(\t);
	open( my $fh, '>:encoding(utf8)', $text_out ) || die "Cannot open $text_out for writing.\n";
	say $fh qq(taxon\t@country_list);
	foreach my $taxon ( sort keys %$countries ) {
		print $fh $taxon;
		foreach my $country (@country_list) {
			print $fh qq(\t) . ( $countries->{$taxon}->{$country} // q() );
		}
		print $fh qq(\n);
	}
	close $fh;
	if ( $opts{'format'} eq 'Excel' ) {
		write_excel_tab( $excel, 'isolate_countries', $text_out );
		unlink $text_out;
	}
}

sub initiate_excel {
	return if $opts{'format'} ne 'Excel';
	my $excel_file = "$opts{'dir'}/PubMLST_stats.xlsx";
	my $workbook   = Excel::Writer::XLSX->new($excel_file);
	$workbook->set_tempdir(TMP_DIR);
	$workbook->set_optimization;
	my $formats = {};
	$formats->{'header'} = $workbook->add_format;
	$formats->{'header'}->set_align('center');
	$formats->{'header'}->set_bold;
	$formats->{'text'} = $workbook->add_format( num_format => '@' );
	$formats->{'text'}->set_align('center');
	$formats->{'cell'} = $workbook->add_format;
	$formats->{'cell'}->set_align('center');

	if ( !defined $workbook ) {
		die "Cannot create Excel file $excel_file\n";
	}
	return { workbook => $workbook, formats => $formats };
}

sub write_excel_tab {
	my ( $excel, $worksheet_name, $text_file ) = @_;
	my ( %text_fields, %text_cols );
	my $workbook = $excel->{'workbook'};
	my $formats  = $excel->{'formats'};

	#Always use text format for likely record names
	$text_fields{$_} = 1 foreach qw(isolate strain sample);
	my $worksheet = $workbook->add_worksheet($worksheet_name);
	open( my $text_fh, '<:encoding(utf8)', $text_file )
	  || throw BIGSdb::CannotOpenFileException("Cannot open $text_file for reading");
	my ( $row, $col ) = ( 0, 0 );
	my %widths;
	my $first_line = 1;
	while ( my $line = <$text_fh> ) {
		$line =~ s/\r?\n$//x;      #Remove terminal newline
		$line =~ s/[\r\n]/ /gx;    #Replace internal newlines with spaces.
		my $format = $row == 0 ? $formats->{'header'} : $formats->{'cell'};
		my @values = split /\t/x, $line;
		foreach my $value (@values) {
			if ( $first_line && $text_fields{$value} ) {
				$text_cols{$col} = 1;
			}
			if ( !$first_line && $text_cols{$col} ) {
				$worksheet->write_string( $row, $col, $value, $formats->{'text'} );
			} else {
				$worksheet->write( $row, $col, $value, $format );
			}
			$widths{$col} = length $value if length $value > ( $widths{$col} // 0 );
			$col++;
		}
		$col = 0;
		$row++;
		$first_line = 0;
	}
	close $text_fh;
	foreach my $col ( keys %widths ) {
		my $width = my $value_width = int( 0.9 * ( $widths{$col} ) + 2 );
		$width = CELL_MAXWIDTH if $width > CELL_MAXWIDTH;
		$worksheet->set_column( $col, $col, $width );
	}
	$worksheet->freeze_panes( 1, 0 );
	return;
}

sub get_counts {
	my ($taxa) = @_;
	my $counts = {};
	my $pm     = Parallel::ForkManager->new( $opts{'threads'} );
	$pm->run_on_finish(
		sub {
			my ( $pid, $exit_code, $ident, $exit_signal, $core_dump, $taxon_data ) = @_;
			$counts->{ $taxon_data->{'taxon'} } = $taxon_data->{'data'} if keys %{ $taxon_data->{'data'} };
		}
	);
	foreach my $taxon ( keys %$taxa ) {
		$pm->start and next;
		my $taxon_data;
		if ( $taxa->{$taxon}->{'isolates'} ) {
			my $isolate_db = get_route( $taxa->{$taxon}->{'isolates'} );
			if ( $isolate_db->{'isolates'} ) {
				my $isolates = get_route( $isolate_db->{'isolates'} );
				$taxon_data->{'isolates'} = $isolates->{'records'};
			}
			if ( $isolate_db->{'genomes'} ) {
				my $isolates = get_route( $isolate_db->{'genomes'} );
				$taxon_data->{'genomes'} = $isolates->{'records'};
			}
		}
		if ( $taxa->{$taxon}->{'seqdef'} ) {
			my $seqdef_db = get_route( $taxa->{$taxon}->{'seqdef'} );
			if ( $seqdef_db->{'loci'} ) {
				my $loci = get_route( $seqdef_db->{'loci'} );
				$taxon_data->{'loci'} = $loci->{'records'};
			}
			if ( $seqdef_db->{'sequences'} ) {
				my $sequences = get_route( $seqdef_db->{'sequences'} );
				$taxon_data->{'sequences'} = $sequences->{'records'};
			}
		}
		$pm->finish( 0, { taxon => $taxon, data => $taxon_data } );
	}
	$pm->wait_all_children;
	return $counts;
}

sub get_country_isolates {
	my ($taxa) = @_;
	my $countries = {};

	#Some databases have historical data - map old country names
	my $country_map = {
		'Czechoslovakia'      => 'Czech Republic',
		'Germany, Baltic Sea' => 'Germany',
		'Germany, North Sea'  => 'Germany',
		'USSR'                => 'Russia'
	};
	my $pm = Parallel::ForkManager->new( $opts{'threads'} );
	$pm->run_on_finish(
		sub {
			my ( $pid, $exit_code, $ident, $exit_signal, $core_dump, $taxon_data ) = @_;
			if ( keys %{ $taxon_data->{'data'}->{'countries'} } ) {
				$countries->{ $taxon_data->{'taxon'} } = $taxon_data->{'data'}->{'countries'};
			}
		}
	);
	foreach my $taxon ( keys %$taxa ) {
		$pm->start and next;
		my $taxon_data = {};
		if ( $taxa->{$taxon}->{'isolates'} ) {
			my $isolate_db = get_route( $taxa->{$taxon}->{'isolates'} );
			if ( $isolate_db->{'fields'} ) {
				my $fields = get_route( $isolate_db->{'fields'} );
				foreach my $field (@$fields) {
					if ( $field->{'name'} eq 'country' && $field->{'breakdown'} ) {
						$taxon_data->{'countries'} = get_route( $field->{'breakdown'} );
						foreach my $mapped_country ( keys %$country_map ) {
							if ( $taxon_data->{'countries'}->{$mapped_country} ) {
								$taxon_data->{'countries'}->{ $country_map->{$mapped_country} } +=
								  $taxon_data->{'countries'}->{$mapped_country};
								delete $taxon_data->{'countries'}->{$mapped_country};
							}
						}
					}
				}
			}
		}
		$pm->finish( 0, { taxon => $taxon, data => $taxon_data } );
	}
	$pm->wait_all_children;
	return $countries;
}

sub get_isolate_dates {
	my ($taxa) = @_;
	my $dates  = {};
	my $pm     = Parallel::ForkManager->new( $opts{'threads'} );
	$pm->run_on_finish(
		sub {
			my ( $pid, $exit_code, $ident, $exit_signal, $core_dump, $taxon_data ) = @_;
			if ( keys %{ $taxon_data->{'data'}->{'dates'} } ) {
				$dates->{ $taxon_data->{'taxon'} } = $taxon_data->{'data'}->{'dates'};
			}
		}
	);
	foreach my $taxon ( keys %$taxa ) {
		$pm->start and next;
		my $taxon_data = {};
		if ( $taxa->{$taxon}->{'isolates'} ) {
			my $isolate_db = get_route( $taxa->{$taxon}->{'isolates'} );
			if ( $isolate_db->{'fields'} ) {
				my $fields = get_route( $isolate_db->{'fields'} );
				foreach my $field (@$fields) {
					if ( $field->{'name'} eq 'date_entered' && $field->{'breakdown'} ) {
						my $taxon_dates = get_route( $field->{'breakdown'} );
						$taxon_data->{'dates'} = extract_date_series($taxon_dates);
					}
				}
			}
		}
		$pm->finish( 0, { taxon => $taxon, data => $taxon_data } );
	}
	$pm->wait_all_children;
	return $dates;
}

sub get_end_dates {
	my @date      = localtime;
	my $end_year  = 1900 + $date[5];
	my $end_month = $date[4] + 1;
	if ( $end_month < 12 ) {
		$end_month++;
	} else {
		$end_year++;
	}
	return ( $end_year, $end_month );
}

sub extract_date_series {
	my ($dates) = @_;
	my ( $end_year, $end_month ) = get_end_dates;
	my $values = {};
	my $count  = 0;
	foreach my $year ( START_YEAR .. $end_year ) {
		foreach my $month ( 1 .. 12 ) {
			my $check_date = sprintf( '%d-%02d-01', $year, $month );
			foreach my $date ( keys %$dates ) {
				if ( $date le $check_date ) {
					$count += delete $dates->{$date};
				}
			}
			$values->{$check_date} = $count;
			last if $year == $end_year && $month == $end_month;
		}
	}
	return $values;
}

sub get_taxa_list {
	my $data    = get_route(URI);
	my $species = {};
	foreach my $group (@$data) {
		my $databases = $group->{'databases'};
		foreach my $db (@$databases) {
			next if $ignore{ $db->{'name'} };
			if ( $db->{'description'} =~ /^([\w\s\.\/\-]*)\sisolates$/x ) {
				my $taxon = $1;
				$species->{$taxon}->{'isolates'} = $db->{'href'};
			}
			if ( $db->{'description'} =~ /^([\w\s\.\/\-]*)\ssequence\/profile\sdefinitions/x ) {
				my $taxon = $1;
				$species->{$taxon}->{'seqdef'} = $db->{'href'};
			}
		}
	}
	return $species;
}

sub get_route {
	my ($url) = @_;
	my $uploader = LWP::UserAgent->new( agent => 'BIGSdb' );
	my ( $response, $available );
	do {
		$available = 1;
		$response  = $uploader->get($url);
		if ( $response->code == 503 ) {    #Service unavailable/busy
			$available = 0;
			sleep 30;
		}
		if ( $response->code == 401 ) {    #Unauthorized
			return {};
		}
	} until ($available);
	if ( $response->is_success ) {
		if ( !$response->content ) {
		}
		my $data = decode_json( $response->content );
		return $data;
	}
	die $response->as_string . qq(\n);
}

sub show_help {
	my $termios = POSIX::Termios->new;
	$termios->getattr;
	my $ospeed = $termios->getospeed;
	my $t = Tgetent Term::Cap { TERM => undef, OSPEED => $ospeed };
	my ( $norm, $bold, $under ) = map { $t->Tputs( $_, 1 ) } qw/me md us/;
	say << "HELP";
${bold}NAME$norm
    ${bold}pubmlst_stats.pl$norm - Generate summary stat files for PubMLST

${bold}--dir$norm [${under}DIRECTORY$norm]
    Output directory - default current directory

${bold}--format$norm [${under}FORMAT$norm]
    Either Excel or text - default text

${bold}--help$norm
    This help page.
    
${bold}--threads$norm [${under}THREADS$norm]
    Threads to use when querying API. Do not set too high or you may overload
    the remote server (and get banned if you are running this as a third 
    party).
HELP
	return;
}
