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

	open(my $pipe, "| dot -Tsvg -o $graphdir/$center_node.svg") or die ("could not fork");

	print $pipe "strict digraph graph1 {\n";

	my %visited_up = traverse($pipe, $center_node, 'UP', $nodesref, $callgraphsref);
	my %visited_down = traverse($pipe, $center_node, 'DOWN', $nodesref, $callgraphsref);

	my %visited_nodes = (%visited_up, %visited_down);

	# we'll print a prettier label for the "center" function
	delete $visited_nodes{$center_node};

	foreach my $node (keys %visited_nodes)
	{
		my $proname = $nodes{$node}{'proname'};
		print $pipe "\"$node\" [label=\"$proname\" URL=\"$node.svg\"];\n";
	}

	print $pipe "\"$center_node\" [label=\"$nodes{$center_node}{'proname'}\", style=dashed];\n";

	print $pipe "}\n";

	close($pipe);
	die("dot failed for graph $center_node") if $? != 0;
}

sub create_per_function_graphs
{
	my $edge;

	my $node;
	my %nodes;
	
	my ($graphdir, $edges_ref, $functions) = @_;

	my @edges = @$edges_ref;

	foreach $edge (@edges)
	{
		my $caller = $edge->{caller};
		my $callee = $edge->{callee};
		my $callgraph = $edge->{callgraphid};

		# skip the top level function edge
		next if $caller == 0;

		if (!exists $nodes{$caller})
		{
			die "function $caller not present in pg_proc" unless exists $functions->{$caller};

			$nodes{$caller} = { predecessors => {},
								successors => { $callee => 1 },
								callgraphs => { $callgraph => 1 },
								proname => $functions->{$caller} };
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
			die "function $callee not present in pg_proc" unless exists $functions->{$callee};

			$nodes{$callee} = { predecessors => { $caller => 1 },
								successors => {},
								callgraphs => { $callgraph => 1},
								proname => $functions->{$callee} };
		}
		else
		{
			$nodes{$callee}{'predecessors'}{$caller} = 1;
			$nodes{$callee}{'callgraphs'}{$callgraph} = 1;
		}
	}

	foreach my $node (keys %nodes)
	{
		draw_graph($graphdir, $node, \%nodes);
	}
}

if (@ARGV != 2)
{
	print "Usage: ./create_per_function_graphs.pl graphdir dbname\n";
	die;
}

my $graphdir = $ARGV[0];
my $dbname = $ARGV[1];

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
	pg_proc
SQL
;

my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "", {RaiseError => 1, PrintError => 0});

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
	die "could not get a list of functions from pg_proc";
}

my %functions;
while (my $row = $sth->fetchrow_hashref)
{
	$functions{$row->{oid}} = $row->{proname};
}

$dbh->disconnect();
$dbh = undef;

create_per_function_graphs($graphdir, \@edges, \%functions);
