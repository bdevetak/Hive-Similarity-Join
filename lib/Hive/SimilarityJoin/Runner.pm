package Hive::SimilarityJoin::Runner;

use strict;
use warnings;

use Time::HiRes qw(time);

my $ONREDUCE;
BEGIN { $ONREDUCE = ($ARGV[0] and $ARGV[0] eq 'reduce'); }

use if !$ONREDUCE, qw(IPC::Cmd run_forked);

use Scalar::Util qw(openhandle);

use File::Basename;
use File::Path qw(
    make_path
    remove_tree
);

use Cwd qw(abs_path);

my $default_tmp_dir = '/tmp/simjoin_tmp_' . $ENV{USER};

# CLASS METHODS
sub new {

    my $class = shift;

    my $params = ref $_[0] eq 'HASH' ? shift : {@_};

    my $self->{config} = $params;

    $self->{config}->{tmp_dir} = $default_tmp_dir
        unless $params->{tmp_dir};

    $self->{config}->{reducer} ||= $0 ;
    $self->{config}->{reducer} = abs_path( $self->{config}->{reducer} );

    my $abs_module_path     = abs_path(__FILE__);
    my($filename, $lib_dir) = fileparse($abs_module_path);

    $self->{config}->{lib_dir} = $lib_dir; # <- needed to copy modules to distributed cache

    $self->{config}->{bucket_size} ||= 5;

    return bless $self, __PACKAGE__;
}

# INSTANCE METHODS
sub run {

    my $self = shift;

    if ($ARGV[0] and $ARGV[0] eq 'reduce') {
        return $self->reduce( @_ );
    }

    remove_tree($self->{config}->{tmp_dir});

    make_path($self->{config}->{tmp_dir})
        or die "Can not create temp directly $self->{config}->{tmp_dir}";

    my $tmp_dir = $self->{config}->{tmp_dir};

    $self -> _generate_hql_file();

    my $out;

    my $out_info = $self->{config}->{out_info};
    my $dirname  = $out_info->{out_dir};
    my $filename = $out_info->{out_file};

    make_path($dirname);

    $out = $dirname . "/" . $filename;

    my $number_of_reducers = $self->{config}->{nr_reducers} || 10;

    # get datasets info
    my $dataset_info     = $self->{config}->{dataset_info};
    my $ref_dataset_info = $dataset_info->{reference_data};

    # export reference dataset
    my ($ref_success, $ref_buffer);
    eval {

        my $ref_outfile;

        my $ref_tsv_file_name = $tmp_dir . '/ref_data.tsv';

        open $ref_outfile, '>', $ref_tsv_file_name
            or die("Couldn't open file: $!");
        if ($ref_outfile && openhandle($ref_outfile)) {
            binmode $ref_outfile;
        }

        my $shell_command;
        {
            $shell_command =
                  "hive -e " .
                  "\"" .
                  $ref_dataset_info->{hql} .
                  "\"";

            print "Going to run shell command => " . $shell_command . "\n";
        }

        my $result = run_forked(
            $shell_command,
            {
                ( (defined $ref_outfile) && (openhandle($ref_outfile)) )
                    ? ( stdout_handler => sub { print $ref_outfile @_ } )
                    : ()
            }
        );
        $ref_buffer  = $result->{stderr};
        $ref_success = !$result->{exit_code};

    } or do {
            print "Hive threw errors: $!" . "\n";
            exit 1;
    };

    # construct and execute hive query
    my ($success, $buffer);
    eval {

        my $outfile;

        my $tsv_file_name = $out;

        open $outfile, '>', $tsv_file_name
            or die("Couldn't open file: $!");
        if ($outfile && openhandle($outfile)) {
            binmode $outfile;
        }

        my $ref_file_name  = 'ref_data.tsv';;

        my $shell_command;
        {
            $shell_command =
                  "hive "
                . "--hiveconf auxiliary_dataset_file=\"$ref_file_name\" "
                . "--hiveconf nr_reducers=$number_of_reducers "
                . "-f $self->{config}->{hql_file_path}"
            ;
            print "Going to run shell command => " . $shell_command . "\n";
        }

        my $result = run_forked(
            $shell_command,
            {
                ( (defined $outfile) && (openhandle($outfile)) )
                    ? ( stdout_handler => sub { print $outfile @_ } )
                    : ()
            }
        );
        $buffer  = $result->{stderr};
        $success = !$result->{exit_code};

    } or do {
            print "Hive threw errors: $!" . "\n";
            exit 1;
    };
}

# _generate_hql_file
sub _generate_hql_file {

    my $self = shift;

    my $config = $self->{config};

    my $tmp_dir = $config->{tmp_dir};

    my $main_script = basename( $self->{config}->{reducer} );

    my $lib_dir = $config->{lib_dir};
    my $runner = "$lib_dir/Runner.pm";

    my $reducer_file = "simjoinrunner.$$." . time . ".pl";

    my $reducer_abs_path = "$tmp_dir/$reducer_file";

    _generate_reducer($main_script, $runner, $reducer_abs_path);

    my $hql_src = <<"---HQL---";
set mapred.reduce.tasks=\${hiveconf:nr_reducers};

ADD FILE $reducer_abs_path;

ADD FILE $tmp_dir/\${hiveconf:auxiliary_dataset_file};

SELECT
    TRANSFORM ( * )

    USING "/usr/bin/perl $reducer_file reduce \${hiveconf:auxiliary_dataset_file}"

    AS (
        id,
        ref_id,
        distance
    )

FROM (
---HQL---

    $hql_src .= $config->{dataset_info}->{main_dataset}->{hql};

    $hql_src .= <<"---HQL---";

    DISTRIBUTE BY
        id

) DATA_TO_PROCESS
---HQL---

    my $hql_file_path = $config->{tmp_dir} . "/hive_simjoin.hql";

    open my $file, '>', $hql_file_path
        or die "can not open file $hql_file_path";

    foreach my $line ( split('\n',$hql_src) ) {
        print $file $line . "\n";
    }

    close $file;

    $self->{config}->{hql_file_path} = $hql_file_path;

}

sub reduce {
    my $self = shift;

    my $reference_dataset = _load_dataset($ARGV[1]) ;

    while (<STDIN>) {
        chomp;

        my @rec = split(/\t/, $_);

        my $top_N_bucket = get_top_N_similar_records($self, \@rec, $reference_dataset) || [];

        foreach my $top_list_rec (@$top_N_bucket) {
            print join ("\t", @$top_list_rec) . "\n";
        }
    }

    exit(0);
}

# _load_dataset
sub _load_dataset {

    my $dataset_file_name = shift;

    print STDERR "trying to open: $dataset_file_name";

    open my $dataset_file, '<', $dataset_file_name
        or die "Could not open dataset file for reading: $!";

    my @data = ();
    while (<$dataset_file>) {
        chomp;
        my @rec = split(/\t/, $_);
        push @data, [@rec];
    }

    close $dataset_file;

    return \@data;

}

sub get_top_N_similar_records {
    my ($self, $row, $reference_dataset) = @_; 

    my $last_index = $self->{config}->{bucket_size} - 1;

    my @bucket_buffer = ();
    my @sorted;
    foreach my $reference_record (@$reference_dataset) {

        # should not compare item to itself
        next if ( $row->[0] eq $reference_record->[0] );

        # similarity_record => [id1, id2, similarity || distance]
        my $similarity_record = main::similarity_distance(
            $row, $reference_record
        );

        next unless $similarity_record;

        # maintain priority queue
        if (scalar @bucket_buffer < $self->{config}->{bucket_size}) {
            push @bucket_buffer, $similarity_record;
            # sorting order is ascending so we get top most similar
            # pairs, that is pairs with minimal distance
            @sorted = sort { $a->[2] <=> $b->[2] } @bucket_buffer;
        }
        else {
            # if last element in the queue has lower similarity score
            # (larger distance) than the current element, replace it
            # with current element, and re-sort the queue
            if ($sorted[$#sorted]->[2] > $similarity_record->[2]) {
                pop @sorted;
                push @sorted, $similarity_record;
                @sorted = sort { $a->[2] <=> $b->[2] } @sorted;
            }
        }

    }

    return \@sorted;;
}

sub _generate_reducer {
    my ($main_abs_path, $runner_abs_path, $reducer_abs_path) = @_;

    my $shell_command = "cat '$main_abs_path' '$runner_abs_path'"
                      . ' | '
                      . "sed '/use[ ]*Hive::SimilarityJoin::Runner/d'"
                      . ' > '
                      . $reducer_abs_path
    ;

    my $success;
    eval {
        my $result = run_forked($shell_command);
        $success = !$result->{exit_code};
    } or do {
        print "Could not generate reducer: $!" . "\n";
        exit 1;
    };
}

1;

__END__

=encoding utf8

=head1 NAME

Hive::SimilarityJoin::Runner - Perl module for doing similarity joins on Hive.

=head1 SYNOPSIS

In the main script file:

    # provide similarity_distance function that will be used to
    # calculate distance between rows during similarity join
    sub similarity_distance {
        my ($row_a, $row_b) = @_;

        my ($id_a,attribute_a_1,...,attribute_a_N) = @$row_a; # <- from big (streamed) table
        my ($id_b,attribute_b_1,...,attribute_b_N) = @$row_b; # <- from small (cached) table

        my $distance;
        # ... do your calc

        return [$id_a, $id_b, $distance];
    }

    use Hive::SimilarityJoin::Runner;

    # create similarity join job
    my $job = Hive::SimilarityJoin::Runner->new({

        # PERF:
        nr_reducers => 100,

        # IN:
        dataset_info => {

            main_dataset => {
                hql => q|
                    SELECT
                        id,
                        attribute_1,...,attribute_N
                    FROM
                        big_table
                |,
            },

            # smaller table should fit in Hadoop distributed cache
            reference_data => {
                hql => q|
                    SELECT
                        id,
                        attribute_1,...,attribute_N
                    FROM
                        small_table
                |,
            },

        },

        # specify N for Top-N bucket:
        bucket_size => 10,

        # OUT:
        out_info => {
            out_dir  => '/tmp/simjoin_out_' . $ENV{USER},
            out_file => 'simjoin_result.tsv',
        },

    });

    # run
    $job->run();

=head1 VERSION

Current version: 0.001

=head1 AUTHOR

bdevetak - Bosko Devetak (cpan:BDEVETAK) <bosko.devetak@gmail.com>

=head1 CONTRIBUTORS

theMage, C<<  <cpan:NEVES> >>, <mailto:themage@magick-source.net>

=head1 COPYRIGHT

Copyright (c) 2014 the Hive::SimilarityJoin L</AUTHOR> and L</CONTRIBUTORS>
as listed above.

=head1 LICENSE

This library is free software and may be distributed under the same terms
as perl itself. See L<http://dev.perl.org/licenses/>.

=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.

=cut

