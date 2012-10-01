#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use DBD::Pg;

package TableUsageGraphs;

BEGIN
{
	require Exporter;

	our $VERSION = 1.00;

	our @ISA = qw(Exporter);

	our @EXPORT = qw(generate_table_usage_graphs);
}

sub draw_graph
{
	my ($graphdir, $toplevelfunction, $graph) = @_;

	open(my $pipe, "| dot -Tsvg -o $graphdir/r$toplevelfunction.svg") or die "could not fork";

	print $pipe "strict digraph $graph->{toplevelfunctionname} {\n";
	print $pipe "rankdir = LR;\nnode [shape=record]\n";

	my $tables = $graph->{tables};
	foreach my $table (keys %{$tables})
	{
		foreach my $referenced_table (@{$tables->{$table}->{confrelids}})
		{
			print $pipe "\"$table\":name -> \"$referenced_table\":name;\n";
		}

		my $tref = $tables->{$table};
		print $pipe "\"$table\" [label=\"<name> $tref->{relname}|" .
					"sequential scans: $tref->{seq_scan}|avg seq tuples: $tref->{seq_tup_read}|" .
					"index scans: $tref->{idx_scan}|avg idx tuples: $tref->{idx_tup_read}\"]\n";
	}

	print $pipe "}\n";

	close($pipe);
	die "dot failed for table graph $toplevelfunction" if $? != 0;

	return scalar keys %{$tables};
}

sub generate_table_usage_graphs
{
	my ($graphdir, $dbh, $system_catalogs) = @_;

	my $table_list_query = 
<<"SQL";
	WITH TopLevelFunctionTableUsage AS (
	    SELECT
	        TopLevelFunction, relid, sum(seq_scan) AS seq_scan, sum(seq_tup_read) AS seq_tup_read,
	                                 sum(idx_scan) AS idx_scan, sum(idx_tup_read) AS idx_tup_read
	    FROM
	        call_graph.TableUsage tu
	    JOIN
	        call_graph.CallGraphs cg
	            ON (tu.CallGraphID = cg.CallGraphID)
	    GROUP BY
	        TopLevelFunction, relid
	)
	SELECT
	    tu.TopLevelFunction AS TopLevelFunction, tlf.proname AS TopLevelFunctionName,
		tu.relid AS relid, pg_class.relname AS relname,
		seq_scan, CASE WHEN seq_scan > 0 THEN round(seq_tup_read / seq_scan::numeric, 2) ELSE 0 END AS seq_tup_read,
		idx_scan, CASE WHEN idx_scan > 0 THEN round(idx_tup_read / idx_scan::numeric, 2) ELSE 0 END AS idx_tup_read,
		pg_constraint.confrelid AS confrelid
	FROM
	    TopLevelFunctionTableUsage tu
	JOIN
	    $system_catalogs->{pg_class} pg_class
	        ON (pg_class.oid = tu.relid)
	JOIN
		$system_catalogs->{pg_proc} tlf
			ON (tu.TopLevelFunction = tlf.oid)
	LEFT JOIN
	    $system_catalogs->{pg_constraint} pg_constraint
	        ON (pg_constraint.contype = 'f' AND pg_constraint.conrelid = tu.relid AND
				-- omit self references
				tu.relid <> pg_constraint.confrelid AND
	            EXISTS (SELECT * FROM TopLevelFunctionTableUsage tu2
	            		WHERE tu2.TopLevelFunction = tu.TopLevelFunction AND
							  tu2.relid = pg_constraint.confrelid))
SQL
	;

	my $sth = $dbh->prepare($table_list_query);
	$sth->execute();

	return undef if ($sth->rows <= 0);

	my %graphs;
	while (my $row = $sth->fetchrow_hashref)
	{
		if (!exists $graphs{$row->{toplevelfunction}})
		{
			$graphs{$row->{toplevelfunction}} = { toplevelfunctionname => $row->{toplevelfunctionname},
												  tables => {} };
		}

		my $tables = $graphs{$row->{toplevelfunction}}{tables};

		if (!exists $tables->{$row->{relid}})
		{
			$tables->{$row->{relid}} = { relname => $row->{relname},
										 seq_scan => $row->{seq_scan},
										 seq_tup_read => $row->{seq_tup_read},
										 idx_scan => $row->{idx_scan},
										 idx_tup_read => $row->{idx_tup_read},
										 confrelids => [] };
		}
		else
		{
			$tables->{$row->{relid}}->{seq_scan} += $row->{seq_scan};
			$tables->{$row->{relid}}->{seq_tup_read} += $row->{seq_tup_read};
			$tables->{$row->{relid}}->{idx_scan} += $row->{idx_scan};
			$tables->{$row->{relid}}->{idx_tup_read} += $row->{idx_tup_read};
		}

		if (defined $row->{confrelid})
		{
			push @{$tables->{$row->{relid}}{confrelids}}, $row->{confrelid};
		}
	}

	foreach my $graph (keys %graphs)
	{
		draw_graph($graphdir, $graph, $graphs{$graph})
			if (scalar keys %{$graphs{$graph}->{tables}} > 0);
	}

	return \%graphs;
}

END
{
}

1;
