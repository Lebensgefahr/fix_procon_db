#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Data::Dumper;


my $debug = $ARGV[0] or die "Use it with debug level ./debug 3";

my $database = 'procon';
my $host = '127.0.0.1';
my $user = 'root';
my $password = '1';

my $dbh = DBI->connect("DBI:mysql:database=$database;host=$host",
                       $user, $password,
                       {'RaiseError' => 1});
my %dogtags;
my %update;
my %soldiers;
my @soldiers_quantity;
my $size;

sub get_player_ids {
    my ($soldier_name) = @_;
    my @player_ids;
    if($debug == 1){print "get_player_ids: SELECT PlayerID FROM (SELECT * FROM tbl_playerdata WHERE SoldierName = '$soldier_name') as firstresult WHERE BINARY SoldierName = '$soldier_name';\n"; }
    my $sth = $dbh->prepare("SELECT PlayerID FROM (SELECT * FROM tbl_playerdata WHERE SoldierName = '$soldier_name') as firstresult WHERE BINARY SoldierName = '$soldier_name'");
    $sth->execute();
    while (my ($player_id) = $sth->fetchrow_array()) {
        push (@player_ids, $player_id);
    }
    $sth->finish();
  return @player_ids;
}

sub get_player_id_by_stats_id {
    my ($stats_id) = @_;
    if($debug == 1){ print "get_player_id_by_stats_id: SELECT PlayerID FROM tbl_server_player WHERE StatsID=$stats_id;\n"; }
    my $sth = $dbh->prepare("SELECT PlayerID FROM tbl_server_player WHERE StatsID=$stats_id");
    $sth->execute();
    my ($player_id) = $sth->fetchrow_array();
    $sth->finish();
  return $player_id;
}

sub get_stats_ids {
    my ($soldier_name) = @_;
    my @player_ids = get_player_ids("$soldier_name");
    my @stats_ids;

    foreach my $player_id (values @player_ids) {
        if($debug == 1){ print "PlayerID:".$player_id."\n"; }
        if($debug == 1){ print "get_stats_ids: SELECT StatsID From tbl_server_player where PlayerID=$player_id;\n"; }
        my $sth = $dbh->prepare("SELECT StatsID FROM tbl_server_player WHERE PlayerID=$player_id");
        $sth->execute();
        while (my ($stats_id) = $sth->fetchrow_array()){
            if($debug == 1){ print "StatsID:".$stats_id."\n"; }
            push (@stats_ids, $stats_id);
        }
        $sth->finish();
    }
    return @stats_ids;
}

#function to fix dogtags killerid
sub fix_tbl_dogtags_killerid {
    my @stats_ids = @_;
    my $old_id = $stats_ids[0];
    my $new_id = $stats_ids[1];

    foreach my $KillerID (keys %dogtags){
        foreach my $VictimID (keys $dogtags{$KillerID}) {
            if((exists $dogtags{$old_id}{$VictimID}) && (exists $dogtags{$new_id}{$VictimID})) {
                $dogtags{$old_id}{$VictimID} = $dogtags{$old_id}{$VictimID} + $dogtags{$new_id}{$VictimID};
                delete $dogtags{$new_id}{$VictimID}
            } elsif (exists $dogtags{$new_id}{$VictimID}) {
                $dogtags{$old_id}{$VictimID} = $dogtags{$new_id}{$VictimID};
                delete $dogtags{$new_id}{$VictimID};
            }
        }
    }
}

#function to fix dogtags victimid
sub fix_tbl_dogtags_victimid {
    my @stats_ids = @_;

    my $old_id = $stats_ids[0];
    my $new_id = $stats_ids[1];

    foreach my $KillerID (keys %dogtags){
        foreach my $VictimID (keys $dogtags{$KillerID}) {
            if ((exists $dogtags{$KillerID}{$old_id}) && (exists $dogtags{$KillerID}{$new_id})){
                $update{$KillerID}{$old_id} = $dogtags{$KillerID}{$old_id} + $dogtags{$KillerID}{$new_id};
                $dogtags{$KillerID}{$old_id} = $dogtags{$KillerID}{$old_id} + $dogtags{$KillerID}{$new_id};
                delete $dogtags{$KillerID}{$new_id};
            }
        }
    }
}

sub fix_tbl_playerstats {
    my @stats_ids = @_;
    my %hash;
    my $old_id = $stats_ids[0];
    my $new_id = $stats_ids[1];
    foreach my $stats_id (@stats_ids) {
        if($debug == 1){ print"fix_tbl_playerstats: SELECT * FROM tbl_playerstats WHERE StatsID=$stats_id;\n"; }
        my $sth = $dbh->prepare("SELECT * FROM tbl_playerstats WHERE StatsID=$stats_id;\n");
        $sth->execute();
#if user did not have stats than remove 
        if(!$sth->fetch()) {
            my $player_id = get_player_id_by_stats_id($stats_id);
	    if($debug == 1) {print "fix_tbl_playerstats: player_id=$player_id\n";}
            if($debug == 1){ print "fix_tbl_playerstats: DELETE FROM tbl_server_player WHERE StatsID=$stats_id\n"; }
            $sth = $dbh->prepare("DELETE FROM tbl_server_player WHERE StatsID=$stats_id;\n");
            $sth->execute();
            $sth->finish();
            if($debug == 1){ print "fix_tbl_playerstats: DELETE FROM adkats_statistics WHERE target_id=$player_id;\n"; }
            $sth = $dbh->prepare("DELETE FROM adkats_statistics WHERE target_id=$player_id;\n");
            $sth->execute();
            $sth->finish();
            if($debug == 1){ print "fix_tbl_playerstats: DELETE FROM tbl_playerdata WHERE PlayerID=$player_id\n"; }
            $sth = $dbh->prepare("DELETE FROM tbl_playerdata WHERE PlayerID=$player_id");
            $sth->execute();
            $sth->finish();
            return 0;
        }
#feel hash with user data
        $sth->execute();
        ($hash{"$stats_id"}{'StatsID'}, $hash{"$stats_id"}{'Score'}, $hash{"$stats_id"}{'Kills'}, $hash{"$stats_id"}{'Headshots'}, $hash{"$stats_id"}{'Deaths'}, 
        $hash{"$stats_id"}{'Suicide'}, $hash{"$stats_id"}{'TKs'}, $hash{"$stats_id"}{'Playtime'}, $hash{"$stats_id"}{'Rounds'}, $hash{"$stats_id"}{'FirstSeenOnServer'},
        $hash{"$stats_id"}{'LastSeenOnServer'}, $hash{"$stats_id"}{'Killstreak'}, $hash{"$stats_id"}{'Deathstreak'}, $hash{"$stats_id"}{'HighScore'}, $hash{"$stats_id"}{'rankScore'},
        $hash{"$stats_id"}{'rankKills'}, $hash{"$stats_id"}{'Wins'}, $hash{"$stats_id"}{'Losses'}) = $sth->fetchrow_array();
        $sth->finish();
    }

#add kills, deaths and other parameters from new id to old
    foreach my $key(keys $hash{$old_id}){
        if(($key ne 'FirstSeenOnServer') && ($key ne 'LastSeenOnServer') && ($key ne 'StatsID') && ($key ne 'HighScore') && ($key ne 'rankScore') && ($key ne 'rankKills')) {
            $hash{$old_id}{$key}=$hash{$old_id}{$key}+$hash{$new_id}{$key};
        }
        if ($key eq 'HighScore') {
            if ($hash{$old_id}{'HighScore'} < $hash{$new_id}{'HighScore'}) {
                $hash{$old_id}{'HighScore'} = $hash{$new_id}{'HighScore'};
            }
        }
        if ($key eq 'rankScore') {
            if ($hash{$old_id}{'rankScore'} > $hash{$new_id}{'rankScore'}) {
                $hash{$old_id}{'rankScore'} = $hash{$new_id}{'rankScore'};
            }
        }
        if ($key eq 'rankKills') {
            if ($hash{$old_id}{'rankKills'} > $hash{$new_id}{'rankKills'}) {
                $hash{$old_id}{'rankKills'} = $hash{$new_id}{'rankKills'};
            }
        }
    }
#update table with calculated data
    my $sth = $dbh->prepare("UPDATE tbl_playerstats SET
                        Score = $hash{$old_id}{'Score'},
                        Kills = $hash{$old_id}{'Kills'},
                        Headshots = $hash{$old_id}{'Headshots'},
                        Deaths = $hash{$old_id}{'Deaths'},
                        Suicide = $hash{$old_id}{'Suicide'},
                        TKs = $hash{$old_id}{'TKs'},
                        Playtime = $hash{$old_id}{'Playtime'},
                        Rounds = $hash{$old_id}{'Rounds'},
                        Killstreak = $hash{$old_id}{'Killstreak'},
                        Deathstreak = $hash{$old_id}{'Deathstreak'},
                        HighScore = $hash{$old_id}{'HighScore'},
                        rankScore = $hash{$old_id}{'rankScore'},
                        rankKills = $hash{$old_id}{'rankKills'},
                        Wins = $hash{$old_id}{'Wins'},
                        Losses = $hash{$old_id}{'Losses'}
                        where StatsID = $old_id");
    $sth->execute();
    $sth->finish();
#delete record with new id
    if($debug == 1){print "fix_tbl_playerstats: DELETE FROM tbl_playerstats WHERE StatsID=$new_id;\n";}
    $sth = $dbh->prepare("DELETE FROM tbl_playerstats WHERE StatsID=$new_id");
    $sth->execute();
    $sth->finish();
}


#this function fixes tbl_dogtags

sub fix_tbl_dogtags {

    my $sth = $dbh->prepare("SELECT * FROM tbl_dogtags");
    $sth->execute();
    while (my ($KillerID, $VictimID, $Count) = $sth->fetchrow_array()) {
        $dogtags{$KillerID}{$VictimID} = $Count;
    }
    $sth->finish();

    my $killer_id_counter = $size;

#change new killers id to an old one
    foreach my $soldier_name (keys %soldiers) {
        if($debug == 3) {print "Fix KillerID for SoldierName: $soldier_name [$killer_id_counter]\n";}
        fix_tbl_dogtags_killerid($soldiers{$soldier_name}{'old'},$soldiers{$soldier_name}{'new'});
        $killer_id_counter--;
    }
    
#change new victims id to an old one
    my $victim_id_counter = $size;
    foreach my $soldier_name (keys %soldiers) {
        if($debug == 3) {print "Fix VictimID for SoldierName: $soldier_name [$victim_id_counter]\n";}
        fix_tbl_dogtags_victimid($soldiers{$soldier_name}{'old'},$soldiers{$soldier_name}{'new'});
        $victim_id_counter--;
    }

#update tables for every hash element
    foreach my $KillerID (keys %update) {
        foreach my $VictimID (keys $update{$KillerID}) {
            if (exists $update{$KillerID}{$VictimID}){
                if($debug == 3){print "UPDATE tbl_dogtags SET Count=$update{$KillerID}{$VictimID} WHERE KillerID=$KillerID AND VictimID=$VictimID;\n";}
                my $sth = $dbh->prepare("UPDATE tbl_dogtags SET Count=$update{$KillerID}{$VictimID} WHERE KillerID=$KillerID AND VictimID=$VictimID");
                $sth->execute();
                $sth->finish();
            }
        }
    }
}

#main program

my $sth = $dbh->prepare("SELECT SoldierName FROM tbl_playerdata WHERE SoldierName IS NOT NULL");
if($debug == 1){ print "SELECT SoldierName FROM tbl_playerdata WHERE SoldierName IS NOT NULL\n"; }
$sth->execute();

while (my ($soldier_name) = $sth->fetchrow_array()) {
    if($debug == 3){ print "Main: Getting StatsID for $soldier_name\n";}
    my @stats_ids = get_stats_ids($soldier_name);
    if (scalar @stats_ids == 2) {
        my $old_id;
        my $new_id;

        if ($stats_ids[0] < $stats_ids[1]) {
            $old_id = $stats_ids[0];
            $new_id = $stats_ids[1];
        } else {
            $old_id = $stats_ids[1];
            $new_id = $stats_ids[0];
        }
        ($soldiers{$soldier_name}{'old'},$soldiers{$soldier_name}{'new'}) = @stats_ids;
    }
}

@soldiers_quantity = keys(%soldiers);
$size = scalar @soldiers_quantity;

fix_tbl_dogtags;

my $playerdata_left = $size;
foreach my $soldier_name (keys %soldiers) {
    if(($debug == 3)||($debug == 1)){ print "Fix tbl_playerdata for SoldierName: $soldier_name with old_id:$soldiers{$soldier_name}{'old'} and new_id:$soldiers{$soldier_name}{'new'} left:[$playerdata_left]\n"; }
    fix_tbl_playerstats($soldiers{$soldier_name}{'old'},$soldiers{$soldier_name}{'new'});
    $playerdata_left--;
}

$sth->finish();
$dbh->disconnect();
#print Dumper \%hash;
#print Dumper \%update;
