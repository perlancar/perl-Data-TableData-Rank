package Data::TableData::Rank;

use 5.010001;
use strict;
use warnings;

# AUTHORITY
# DATE
# DIST
# VERSION

use Exporter qw(import);
our @EXPORT_OK = qw(add_rank_column_to_table);

our %SPEC;

$SPEC{add_rank_column_to_table} = {
    v => 1.1,
    summary => 'Add a rank column to a table',
    description => <<'_',

Will modify the table by adding a rank column. An example, with this table:

    | name       | gold | silver | bronze |
    |------------+------+--------+--------|
    | E          |  2   |  5     |  7     |
    | A          | 10   | 20     | 15     |
    | H          |  0   |  0     |  1     |
    | B          |  8   | 23     | 17     |
    | G          |  0   |  0     |  1     |
    | J          |  0   |  0     |  0     |
    | C          |  4   |  9     |  8     |
    | D          |  4   | 10     | 13     |
    | I          |  0   |  0     |  1     |
    | F          |  2   |  5     |  1     |

the result of ranking the table with data columns of C<<
["gold","silver","bronze"] >> will be:

    | name       | gold | silver | bronze | rank |
    |------------+------+--------+--------+------|
    | A          | 10   | 20     | 15     |  1   |
    | B          |  8   | 23     | 17     |  2   |
    | C          |  4   |  9     |  8     |  3   |
    | D          |  4   | 10     | 13     |  4   |
    | E          |  2   |  5     |  7     |  5   |
    | F          |  2   |  5     |  1     |  6   |
    | G          |  0   |  0     |  1     | =7   |
    | H          |  0   |  0     |  1     | =7   |
    | I          |  0   |  0     |  1     | =7   |
    | J          |  0   |  0     |  0     | 10   |

_
    args => {
        table => {
            summary => 'A table data (either aoaos, aohos, or its Data::TableData::Object wrapper)',
            schema => 'any*',
            req => 1,
        },
        data_columns => {
            summary => 'Array of names (or indices) of columns which contain the data to be compared, which must all be numeric',
            schema => [array => {of => 'str*', min_len=>1}],
            req => 1,
        },
        smaller_wins => {
            summary => 'Whether a smaller number in the data wins; normally a bigger name means a higher rank',
            schema => 'bool*',
            default => 0,
        },
        rank_column_name => {
            schema => 'str*',
            default => 'rank',
        },
        add_equal_prefix => {
            schema => 'bool*',
            default => 1,
        },
        rank_column_idx => {
            schema => 'int*',
        },
    },
};
sub add_rank_column_to_table {
    require Data::TableData::Object;

    my %args = @_;
    my $data_columns = $args{data_columns};
    my $smaller_wins = $args{smaller_wins} // 0;
    my $add_equal_prefix = $args{add_equal_prefix} // 1;
    my $rank_column_name = $args{rank_column_name} // 'rank';

    my $td = Data::TableData::Object->new($args{table});
    my @colidxs = map { $td->col_idx($_) } @$data_columns;

    my $aoaos = $td->rows_as_aoaos;
    my $cmp_row = sub {
        my ($row1, $row2) = @_;
        for (@colidxs) {
            my $cmp = $row1->[$_] <=> $row2->[$_];
            $cmp = -$cmp unless $smaller_wins;
            return $cmp if $cmp;
        }
        0;
    };
    my @sorted_aoaos = sort { $cmp_row->($a, $b) } @$aoaos;

    my @ranks;
    my %num_has_rank; # key=rank, val=num of rows
    for my $rownum (0 .. $#sorted_aoaos) {
        if ($rownum) {
            if ($cmp_row->($sorted_aoaos[$rownum-1], $sorted_aoaos[$rownum])) {
                my $rank = @ranks + 1;
                push @ranks, $rank;
                $num_has_rank{$rank}++;
            } else {
                push @ranks, $ranks[-1];
                $num_has_rank{ $ranks[-1] }++;
            }
        } else {
            push @ranks, 1;
            $num_has_rank{1}++;
        }
    }

    if ($add_equal_prefix) {
        for my $i (0..$#ranks) {
            if ($num_has_rank{ $ranks[$i] } > 1) { $ranks[$i] = "=$ranks[$i]" }
        }
    }

    $td->add_col($rank_column_name, $args{rank_column_idx}, {}, \@ranks);
    $td;
}

1;
# ABSTRACT:

=head1 SEE ALSO

L<Data::TableData::Object>
