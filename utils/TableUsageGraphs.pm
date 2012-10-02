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

sub draw_relationgraph
{
	my ($graphdir, $relid, $tlfgraphs, $relation) = @_;

	open(my $pipe, "| dot -Tsvg -o $graphdir/r$relid.svg") or die "could not fork";

	print $pipe "strict digraph $relation->{relname} {\n";
	print $pipe "rankdir = LR;\n";

	foreach my $tlf (@{$relation->{toplevelfunctions}})
	{
		print $pipe "\"$tlf\" -> \"$relid\":name;\n";
		print $pipe "\"$tlf\" [label=\"$tlfgraphs->{$tlf}->{toplevelfunctionname}\" href=\"tlf$tlf.svg\"]\n";
	}

	print $pipe "\"$relid\" [label=\"<name> $relation->{relname}|" .
				"sequential scans: $relation->{seq_scan}|avg seq tuples: $relation->{seq_tup_read}|" .
				"index scans: $relation->{idx_scan}|avg idx tuples: $relation->{idx_tup_read}|" .
				"inserted rows: $relation->{n_tup_ins}|updated rows: $relation->{n_tup_upd}|" .
				"deleted rows: $relation->{n_tup_del}\", shape=record]\n";

	print $pipe "}\n";

	close($pipe);
	die "dot failed for relation graph $relation->{relid}" if $? != 0;
}

sub draw_tlfgraph
{
	my ($graphdir, $toplevelfunction, $graph) = @_;

	open(my $pipe, "| dot -Tsvg -o $graphdir/tlf$toplevelfunction.svg") or die "could not fork";

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
					"index scans: $tref->{idx_scan}|avg idx tuples: $tref->{idx_tup_read}|" .
					"inserted rows: $tref->{n_tup_ins}|updated rows: $tref->{n_tup_upd}|" .
					"deleted rows: $tref->{n_tup_del}\", href=\"r$table.svg\"]\n";
	}

	print $pipe "}\n";

	close($pipe);
	die "dot failed for table graph $toplevelfunction" if $? != 0;
}

sub generate_table_usage_graphs
{
	my ($graphdir, $dbh, $system_catalogs) = @_;

	my $table_list_query = 
<<"SQL";
	WITH TopLevelFunctionTableUsage AS (
	    SELECT
	        TopLevelFunction, relid, sum(seq_scan) AS seq_scan, sum(seq_tup_read) AS seq_tup_read,
	                                 sum(idx_scan) AS idx_scan, sum(idx_tup_read) AS idx_tup_read,
									 sum(n_tup_ins) AS n_tup_ins, sum(n_tup_upd) AS n_tup_upd,
									 sum(n_tup_del) AS n_tup_del
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
		pg_constraint.confrelid AS confrelid,
		seq_scan, CASE WHEN seq_scan > 0 THEN round(seq_tup_read / seq_scan::numeric, 2) ELSE 0 END AS seq_tup_read,
		idx_scan, CASE WHEN idx_scan > 0 THEN round(idx_tup_read / idx_scan::numeric, 2) ELSE 0 END AS idx_tup_read,
		n_tup_ins, n_tup_upd, n_tup_del
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

	my $per_relation_stats_query = 
<<"SQL"
	SELECT
		relid, relname, TopLevelFunctions,
		seq_scan, CASE WHEN seq_scan > 0 THEN round(seq_tup_read / seq_scan::numeric, 2) ELSE 0 END AS seq_tup_read,
		idx_scan, CASE WHEN idx_scan > 0 THEN round(idx_tup_read / idx_scan::numeric, 2) ELSE 0 END AS idx_tup_read,
		n_tup_ins, n_tup_upd, n_tup_del
	FROM
	(
		SELECT
			relid, pg_class.relname AS relname, array_agg(TopLevelFunction) AS TopLevelFunctions,
			sum(seq_scan) AS seq_scan, sum(seq_tup_read) AS seq_tup_read,
			sum(idx_scan) AS idx_scan, sum(idx_tup_read) AS idx_tup_read,
			sum(n_tup_ins) AS n_tup_ins, sum(n_tup_upd) AS n_tup_upd,
			sum(n_tup_del) AS n_tup_del
		FROM
			call_graph.TableUsage tu
		JOIN
			call_graph.CallGraphs cg
				ON (tu.CallGraphID = cg.CallGraphID)
		JOIN
		    $system_catalogs->{pg_class} pg_class
	   	     ON (pg_class.oid = tu.relid)
		GROUP BY
			relid, pg_class.relname
	) ss
SQL
	;

	my $sth = $dbh->prepare($table_list_query);
	$sth->execute();

	return undef if ($sth->rows <= 0);

	my %tlfgraphs;
	while (my $row = $sth->fetchrow_hashref)
	{
		if (!exists $tlfgraphs{$row->{toplevelfunction}})
		{
			$tlfgraphs{$row->{toplevelfunction}} = { toplevelfunctionname => $row->{toplevelfunctionname},
													 tables => {} };
		}

		my $tables = $tlfgraphs{$row->{toplevelfunction}}{tables};

		if (!exists $tables->{$row->{relid}})
		{
			$tables->{$row->{relid}} = { relname => $row->{relname},
										 confrelids => [],
										 seq_scan => $row->{seq_scan},
										 seq_tup_read => $row->{seq_tup_read},
										 idx_scan => $row->{idx_scan},
										 idx_tup_read => $row->{idx_tup_read},
										 n_tup_ins => $row->{n_tup_ins},
										 n_tup_upd => $row->{n_tup_upd},
										 n_tup_del => $row->{n_tup_del}
										 };
		}
		
		# Note that if the row exists, we don't want to do anything to the statistics;
		# those are duplicated for each confrelid and summing them would be wrong.

		if (defined $row->{confrelid})
		{
			push @{$tables->{$row->{relid}}{confrelids}}, $row->{confrelid};
		}
	}

	$sth = $dbh->prepare($per_relation_stats_query);
	$sth->execute();

	my %relationgraphs;
	while (my $row = $sth->fetchrow_hashref)
	{
		$relationgraphs{$row->{relid}} = $row;
	}

	foreach my $graph (keys %tlfgraphs)
	{
		draw_tlfgraph($graphdir, $graph, $tlfgraphs{$graph})
			if (scalar keys %{$tlfgraphs{$graph}->{tables}} > 0);
	}

	foreach my $relation (keys %relationgraphs)
	{
		draw_relationgraph($graphdir, $relation, \%tlfgraphs, $relationgraphs{$relation});
	}

	return \%tlfgraphs;
}

END
{
}

1;
