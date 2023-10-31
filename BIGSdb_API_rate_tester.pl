#!/usr/bin/env perl
#Simple script to test rate-limiting on PubMLST API.
#This script forks to make concurrent API calls to PubMLST. Calls over the
#concurrent connection limit (currently 4) should be rejected with a 429
#(too many requests) error.
#Written by Keith Jolley 2023
#License: GPL3
#Version: 2023-10-31.
use 5.010;
use REST::Client;
use JSON;
use Parallel::ForkManager;
use Data::Dumper;
my $base_url  = 'https://rest.pubmlst.org';
my $processes = 5;
my $client    = REST::Client->new();
my $url       = "$base_url/db/pubmlst_neisseria_isolates/isolates?return_all=1";
my $response  = call($url);
say "$response->{'status'}: Isolate list";
my $isolates = $response->{'isolates'};
my $pm       = Parallel::ForkManager->new($processes);

foreach my $isolate (@$isolates) {
	$pm->start and next;
	my $id;
	if ( $isolate =~ /\/(\d+)$/x ) {
		$id = $1;
	} else {
		$id = 'INVALID ID';
	}
	$response = call($isolate);
	say "$response->{'status'}: id-$id";
	$pm->finish;
}
$pm->wait_all_children;

sub call {
	my ( $url, $method ) = @_;
	$method //= 'GET';
	$client->request( 'GET', $url );
	my $status   = $client->responseCode // 200;
	my $response = {};
	if ( $status == 200 ) {
		$response = from_json( $client->responseContent );
	}
	$response->{'status'} //= $status;
	return $response;
}
