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
package ua.ac.be.fpm;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import ua.ac.be.fpm.bigfim.AprioriPhaseMapperTest;
import ua.ac.be.fpm.bigfim.AprioriPhaseReducerTest;
import ua.ac.be.fpm.bigfim.ComputeTidListMapperTest;
import ua.ac.be.fpm.bigfim.ComputeTidListReducerTest;
import ua.ac.be.fpm.eclat.EclatMinerTest;
import ua.ac.be.fpm.eclat.util.ItemTest;
import ua.ac.be.fpm.eclat.util.SplitByKTextInputFormatTest;
import ua.ac.be.fpm.eclat.util.TrieDumperTest;
import ua.ac.be.fpm.util.DbTransposerTest;
import ua.ac.be.fpm.util.ToolsTest;

@RunWith(Suite.class)
@SuiteClasses({FPMDriversTest.class, AprioriPhaseMapperTest.class, AprioriPhaseReducerTest.class,
    ComputeTidListMapperTest.class, ComputeTidListReducerTest.class, EclatMinerTest.class, ItemTest.class,
    SplitByKTextInputFormatTest.class, TrieDumperTest.class, DbTransposerTest.class, ToolsTest.class})
public class AllTests {}
