#!/usr/bin/perl
#use strict;
use warnings FATAL => 'all';

open(IN, $ARGV[0]) || die $!;
open(OUT, ">$ARGV[0]\.parsed") || die $!;
while (<IN>){
    if ($_ =~ /<\/s>/){
        print OUT "\n";
    }
    elsif ($_ !~ /<s>|<text.*|<\/text>|[\.,:;\(\)\"\']/){
        chomp $_;
        print OUT "$_ ";
    }
}
close(IN);
close(OUT);
