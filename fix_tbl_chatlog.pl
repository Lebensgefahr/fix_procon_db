#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Data::Dumper;

my $database = 'procon';
my $host = '127.0.0.1';
my $user = 'root';
my $password = '1';
my $soldier_name;

my $debug=0;

my %hash;

my $dbh = DBI->connect("DBI:mysql:database=$database;host=$host",
                       $user, $password,
                       {'RaiseError' => 1});

my $sth = $dbh->prepare("SELECT DISTINCT logSoldierName, logPlayerID FROM tbl_chatlog");
$sth->execute();

while (my ($soldier_name, $player_id) = $sth->fetchrow_array()) {
    if(!defined $player_id) {
        $hash{$soldier_name}{'NULL'} = 'NULL';
    } else {
        $hash{$soldier_name}{$player_id} = "$player_id";
    }
    print "Executing for soldier $soldier_name\n";
}

foreach my $soldier_name (keys %hash) {
    my @keys_qnt = keys($hash{$soldier_name});
    if(scalar @keys_qnt == 2){
        foreach my $player_id (keys $hash{$soldier_name}){
            if($player_id ne "NULL"){
                print $player_id."\n";
                print "Setting id=$player_id for soldier $soldier_name\n";
                my $sth = $dbh->prepare("UPDATE tbl_chatlog SET logPlayerID=$player_id WHERE BINARY logSoldierName='$soldier_name' AND logPlayerID IS NULL");
                $sth->execute();
                $sth->finish();
            }
        }
    }
}

$sth->finish();
$dbh->disconnect();
#print Dumper \%hash;

