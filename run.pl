#!/usr/bin/perl
use strict;
use warnings;

use 5.010;

use lib "lib/Hive/SimilarityJoin";
use lib ".";

use Runner;

use Math::Trig qw(great_circle_distance :pi);

my $rho=6371;
my $degToRad = pi / 180.0;

# Simple example that calculates geo distances between cities
my $job = Hive::SimilarityJoin::Runner->new({

    # PERF:
    nr_reducers => 100,

    # IN:
    dataset_info => {

        main_dataset => {
            hql => q|
                SELECT
                    id,
                    latitude,
                    longitude
                FROM
                    my_city_table
                WHERE
                    cc1 = 'nl'
            |,
        },

        reference_data => {
            hql          => q|
                SELECT
                    id,
                    latitude,
                    longitude
                FROM
                    my_city_table
                WHERE
                    cc1 = 'nl'
            |,
        },

    },

    bucket_size => 10,

    # OUT:
    out_info => {
        out_dir  => '/tmp/simjoin_out_' . $ENV{USER},
        out_file => 'simjoin_result.tsv',
    },

});

$job->run();

sub similarity_distance {
    my ($row, $ref_row) = @_;
    my (
      $id_1,
      $lat_1,
      $long_1
    ) = @{$row};

    my (
      $id_2,
      $lat_2,
      $long_2
    ) = @{$ref_row};

    return if $id_1 == $id_2;

    # convert to radians
    my $long_1_radians = $long_1 * $degToRad || 0;
    my $lat_1_radians  = $lat_1  * $degToRad || 0;
    my $long_2_radians = $long_2 * $degToRad || 0;
    my $lat_2_radians  = $lat_2  * $degToRad || 0;

    my $geo_distance = 0;
    $geo_distance = great_circle_distance(
      $long_1_radians,
      pi/2 - $lat_1_radians,
      $long_2_radians,
      pi/2 - $lat_2_radians,
      $rho
    );  

    return [$id_1,$id_2, $geo_distance];
}
