/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package be.uantwerpen.adrem;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import be.uantwerpen.adrem.bigfim.AprioriPhaseMapperTest;
import be.uantwerpen.adrem.bigfim.AprioriPhaseReducerTest;
import be.uantwerpen.adrem.bigfim.ComputeTidListMapperTest;
import be.uantwerpen.adrem.bigfim.ComputeTidListReducerTest;
import be.uantwerpen.adrem.eclat.EclatMinerTest;
import be.uantwerpen.adrem.eclat.util.ItemTest;
import be.uantwerpen.adrem.eclat.util.SplitByKTextInputFormatTest;
import be.uantwerpen.adrem.eclat.util.TrieDumperTest;
import be.uantwerpen.adrem.util.DbTransposerTest;
import be.uantwerpen.adrem.util.ToolsTest;

@RunWith(Suite.class)
@SuiteClasses({
	FPMDriversTest.class, 
	AprioriPhaseMapperTest.class, 
	AprioriPhaseReducerTest.class,
	ComputeTidListMapperTest.class, 
	ComputeTidListReducerTest.class, 
	EclatMinerTest.class, 
	ItemTest.class,
	SplitByKTextInputFormatTest.class, 
	TrieDumperTest.class, 
	DbTransposerTest.class, 
	ToolsTest.class})
public class AllTests {}
