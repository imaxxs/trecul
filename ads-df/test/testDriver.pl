#
# Copyright (c) 2012, Akamai Technologies
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
#   Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
# 
#   Redistributions in binary form must reproduce the above
#   copyright notice, this list of conditions and the following
#   disclaimer in the documentation and/or other materials provided
#   with the distribution.
# 
#   Neither the name of the Akamai Technologies nor the names of its
#   contributors may be used to endorse or promote products derived
#   from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
#

use strict;

use File::Temp;
use IO::File;

my $AdsDf="";

#
# Run a script as a compile/plan pair to
# test plan serialization.
#
sub runScriptSerializedPlan {
    my $testScript = shift;
    my $plan = `$AdsDf --file $testScript --compile`;
    my $tmpPlan = File::Temp->new();
    print $tmpPlan $plan;
    $tmpPlan->close();
    my @args = ("$AdsDf", "--file", "$tmpPlan", "--plan");
    system(@args);
}

#
# Run script without serialization.
#
sub runScript {
    my $testScript = shift;
    my @args = ("$AdsDf", "--file", "$testScript");
    system(@args);
}

#
# Run all tests
#
sub runTests {
    my $testDirs = shift;
    my $failedTests = shift;
    my $runSerialized = shift;
    #
    # foreach test
    #
    my $nPassed = 0;
    my $nFailed = 0;
    my($testDir);

    foreach $testDir (@$testDirs)
    {
	my $failed = 0;
	chdir $testDir;
	opendir(DIR, ".");
	my @contents = readdir(DIR);
	closedir(DIR);
	my @testScripts = grep(/.iql$/, @contents);
	my $testScript;
	foreach $testScript (@testScripts) {
	    if ($runSerialized) {
		&runScriptSerializedPlan($testScript);
	    } else {
		&runScript($testScript);
	    }
	}
	my @expectedOutputs = grep(/expected$/, @contents);
	my $expectedOutput;
	foreach $expectedOutput (@expectedOutputs) {
	    # Find generated matching output by removing trailing .expected.
	    my $generatedOutput = $expectedOutput;
	    $generatedOutput =~ s/.expected$//;
	    my @args =("diff", "$generatedOutput", "$expectedOutput");
	    system(@args);
	    if ($? != 0) {
		$failed = 1;
	    } else {
		# Cleanup file if success
		unlink($generatedOutput);
	    }
	}
	chdir "..";
	if ($failed==1) {
	    push(@$failedTests, $testDir);
	    $nFailed++;
	} else {
	    $nPassed++;
	}
    }
    return ($nPassed, $nFailed);
}

my @testDirs;
print $ARGV[0]."\n";
chdir($ARGV[0]);
opendir(DIR, ".");
@testDirs = grep(/test[0-9]+$/,readdir(DIR));
closedir(DIR);

$AdsDf = $ARGV[1];

my $totalFailures = 0;
for (my $i = 0; $i < 2; ++$i) {
    print "Running tests with serialized=$i\n";
    my @failedTests;
    my ($nPassed, $nFailed) = runTests(\@testDirs, \@failedTests, $i);
    
    print "\nNumber of tests passed: $nPassed\n";
    print "Number of tests failed: $nFailed\n";
    
    if ($nFailed > 0) {
	my $failedTest;
	foreach $failedTest (@failedTests) {
	    print "$failedTest: failed\n";
	}
    }
    $totalFailures += $nFailed;
}    

if ($totalFailures) {
    die "Unit tests failed";
}
