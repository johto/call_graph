#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use DBD::Pg;


# return true if the two input hashes have any keys in common, false otherwise
sub hash_overlap
{
	my ($aref, $bref) = @_;

	my %a = %$aref;
	my %b = %$bref;

	my %c = (%a, %b);

	return scalar keys %c < (scalar keys %a) + (scalar keys %b);
}

# When traversing through the tree, we want to get skip any nodes that don't
# belong to any of the call graphs we're interested in to avoid polluting the
# graph with unrelated nodes.
sub remove_unrelated_neighbours
{
	my ($nref, $nodesref, $callgraphsref) = @_;

	my @neighbours = @$nref;
	my %nodes = %$nodesref;

	return grep { hash_overlap($callgraphsref, $nodes{$_}{'callgraphs'}) } @neighbours;
}

sub traverse
{
	my ($output, $start_node, $direction, $nodes, $callgraphsref) = @_;

	my $node;
	my @callgraphs = keys %$callgraphsref;

	#
	# We keep track of visited nodes for two reasons:
	#   1) It avoids infinite recursion when cycles are present
	#   2) We know which labels to add to the dot file
	#
	# Note that we might still draw end up printing the same edge twice.  We simply tell
	# dot to ignore the second one in case that happens.
	#
	my %visited_nodes;

	my @nodelist = $start_node;
	my @new_nodelist;
	while (1)
	{
		undef @new_nodelist;

		foreach $node (@nodelist)
		{
			my @neighbours;

			# if we've already visited this node, just skip it (see above)
			next if (exists $visited_nodes{$node});
			$visited_nodes{$node} = 1;

			if ($direction eq 'DOWN')
				{ @neighbours = keys %{$nodes->{$node}{'successors'}}; }
			elsif ($direction eq 'UP')
				{ @neighbours = keys %{$nodes->{$node}{'predecessors'}}; }
			else
				{ die "unknown traverse direction $direction"; }

			next if scalar @neighbours == 0;

			# remove neighbours that aren't part of any callgraphs we're interested in (see
			# comment in remove_unrelated_neighbours)
			@neighbours = remove_unrelated_neighbours(\@neighbours, $nodes, $callgraphsref);

			foreach my $neighbour (@neighbours)
			{
				if ($direction eq 'UP')
					{ print $output "\"$neighbour\" -> \"$node\";\n"; }
				else
					{ print $output "\"$node\" -> \"$neighbour\";\n"; }
			}

			push @new_nodelist, @neighbours;
		}

		last if scalar @new_nodelist == 0;

		# Remove any duplicates we might have gathered.  This is not strictly necessary
		# since we keep track of visited nodes anyway, but it won't hurt.
		my %f = map { $_, 1 } @new_nodelist;
		@nodelist = keys %f;
	}

	return %visited_nodes;
}

sub draw_graph
{
	my ($graphdir, $center_node, $nodesref) = @_;

	my %nodes = %$nodesref;
	my $callgraphsref = $nodes{$center_node}{'callgraphs'};

	open(my $pipe, "| dot -Tsvg -o $graphdir/$center_node.svg") or die "could not fork";

	print $pipe "strict digraph graph1 {\n";

	my %visited_up = traverse($pipe, $center_node, 'UP', $nodesref, $callgraphsref);
	my %visited_down = traverse($pipe, $center_node, 'DOWN', $nodesref, $callgraphsref);

	my %visited_nodes = (%visited_up, %visited_down);

	# we'll print a prettier label for the "center" function
	delete $visited_nodes{$center_node};

	foreach my $node (keys %visited_nodes)
	{
		my $proname = $nodes{$node}{'proname'};
		my $penwidth = $nodes{$node}{'istoplevelfunction'} ? 2.5 : 1.0;
		print $pipe "\"$node\" [label=\"$proname\" URL=\"$node.svg\" penwidth=\"$penwidth\"];\n";
	}

	print $pipe "\"$center_node\" [label=\"$nodes{$center_node}{'proname'}\", style=dashed];\n";

	print $pipe "}\n";

	close($pipe);
	die "dot failed for graph $center_node" if $? != 0;
}

sub generate_per_function_graphs
{
	my $edge;

	my $node;
	my %nodes;
	
	my ($graphdir, $dbh, $oid_lookup_table) = @_;

	my $edge_query = 
<<"SQL";
	SELECT
		Caller, Callee, CallGraphID
	FROM
		call_graph.Edges
SQL
	;

	my $oid_name_map_query =
<<"SQL";
	SELECT
		oid, proname
	FROM
		$oid_lookup_table
SQL
	;

	my $sth = $dbh->prepare($edge_query);
	$sth->execute();

	if ($sth->rows <= 0)
	{
		# shouldn't happen
		die "no edges found in call_graph.Edges";
	}

	my @edges;
	while (my $row = $sth->fetchrow_hashref)
	{
		push @edges, $row;
	}

	$sth = $dbh->prepare($oid_name_map_query);
	$sth->execute();

	if ($sth->rows <= 0)
	{
		# shouldn't happen
		die "could not get a list of functions from $oid_lookup_table";
	}

	my %functions;
	while (my $row = $sth->fetchrow_hashref)
	{
		$functions{$row->{oid}} = $row->{proname};
	}

	# First populate %nodes with information about all the nodes.
	foreach $edge (@edges)
	{
		my $caller = $edge->{caller};
		my $callee = $edge->{callee};
		my $callgraph = $edge->{callgraphid};

		# If this is the first edge of the call graph, just make sure we know that
		# the function is a top level function and then skip to the next edge.
		if ($caller == 0)
		{
			if (!exists $nodes{$callee})
			{
				die "function $callee not present in oid lookup table" unless exists $functions{$callee};
			
				$nodes{$callee} = { predecessors => {},
									successors => {},
									callgraphs => { $callgraph => 1},
									istoplevelfunction => 1,
									proname => $functions{$callee} };
			}
			else
			{
				$nodes{$callee}{'callgraphs'}{$callgraph} = 1;
				$nodes{$callee}{'istoplevelfunction'} = 1;
			}

			next;
		}

		if (!exists $nodes{$caller})
		{
			die "function $caller not present in oid lookup table" unless exists $functions{$caller};

			$nodes{$caller} = { predecessors => {},
								successors => { $callee => 1 },
								callgraphs => { $callgraph => 1 },
								proname => $functions{$caller},
								istoplevelfunction => 0 };
		}
		else
		{
			$nodes{$caller}{'successors'}{$callee} = 1;
			$nodes{$caller}{'callgraphs'}{$callgraph} = 1;
		}

		# only set the successor for recursive functions
		next if ($caller == $callee);

		if (!exists $nodes{$callee})
		{
			die "function $callee not present in oid lookup table" unless exists $functions{$callee};

			$nodes{$callee} = { predecessors => { $caller => 1 },
								successors => {},
								callgraphs => { $callgraph => 1},
								proname => $functions{$callee},
								istoplevelfunction => 0 };
		}
		else
		{
			$nodes{$callee}{'predecessors'}{$caller} = 1;
			$nodes{$callee}{'callgraphs'}{$callgraph} = 1;
		}
	}

	# .. and finally, draw all of the graphs
	foreach my $node (keys %nodes)
	{
		# don't generate graphs for top level functions; create_graphs.pl would
		# just overwrite the ones we drew
		next if $nodes{$node}{'istoplevelfunction'};

		draw_graph($graphdir, $node, \%nodes);
	}
}
1;
