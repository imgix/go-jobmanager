#!/usr/bin/perl
use strict;
use warnings;
use MIME::Base64;
use Encode;

binmode(STDOUT);
foreach (@ARGV) {
	my $data;

	{
	  local $/ = undef;
	  open FILE, $_ or die "Couldn't open file: $! $_";
	  binmode FILE;
	  $data = <FILE>;
	  close FILE;
	}
	$data = Encode::encode_utf8($data);
	my $len = length($data);
	syswrite(STDERR, "perl job payload length: $len\n");
	$len = pack("s", $len);
	syswrite(STDOUT, $len . $data);

}
