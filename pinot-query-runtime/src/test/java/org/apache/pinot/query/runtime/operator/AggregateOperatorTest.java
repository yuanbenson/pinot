/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;

public class AggregateOperatorTest {

  private AutoCloseable _mocks;

  @Mock
  private MultiStageOperator _input;

  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldHandleUpstreamErrorBlocks() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    Mockito.when(_input.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!")));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, inSchema, calls, group,
            AggType.INTERMEDIATE, null, null);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build

    // Then:
    Mockito.verify(_input, Mockito.times(1)).nextBlock();
    Assert.assertTrue(block1.isErrorBlock(), "Input errors should propagate immediately");
  }

  @Test
  public void shouldHandleEndOfStreamBlockWithNoOtherInputs() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    Mockito.when(_input.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, inSchema, calls, group,
            AggType.LEAF, null, null);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(1)).nextBlock();
    Assert.assertTrue(block.isEndOfStreamBlock(), "EOS blocks should propagate");
  }

  @Test
  public void shouldHandleUpstreamNoOpBlocksWhileConstructing() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 1}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, inSchema, calls, group,
            AggType.LEAF, null, null);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build when reading NoOp block
    TransferableBlock block2 = operator.nextBlock(); // return when reading EOS block

    // Then:
    Mockito.verify(_input, Mockito.times(3)).nextBlock();
    Assert.assertTrue(block1.isNoOpBlock());
    Assert.assertEquals(block2.getContainer().size(), 1);
  }

  @Test
  public void shouldAggregateSingleInputBlock() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, inSchema, calls, group,
            AggType.INTERMEDIATE, null, null);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1.0},
        "Expected two columns (group by key, agg value), agg value is intermediate type");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void shouldAggregateSingleInputBlockWithLiteralInput() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.Literal(FieldSpec.DataType.DOUBLE, 1.0)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3.0}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, LONG});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, inSchema, calls, group,
            AggType.FINAL, null, null);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    // second value is 1 (the literal) instead of 3 (the col val)
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1L},
        "Expected two columns (group by key, agg value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testGroupByAggregateWithHashCollision() {
    MultiStageOperator upstreamOperator = OperatorTestUtil.getOperator(OperatorTestUtil.OP_1);
    // Create an aggregation call with sum for first column and group by second column.
    RexExpression.FunctionCall agg = getSum(new RexExpression.InputRef(0));
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{STRING, INT});
    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{STRING, DOUBLE});
    AggregateOperator sum0GroupBy1 = new AggregateOperator(OperatorTestUtil.getDefaultContext(), upstreamOperator,
        outSchema, inSchema, Collections.singletonList(agg),
        Collections.singletonList(new RexExpression.InputRef(1)), AggType.LEAF, null, null);
    TransferableBlock result = sum0GroupBy1.getNextBlock();
    while (result.isNoOpBlock()) {
      result = sum0GroupBy1.getNextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    Assert.assertEquals(resultRows.size(), 2);
    if (resultRows.get(0).equals("Aa")) {
      Assert.assertEquals(resultRows.get(0), new Object[]{"Aa", 1.0});
      Assert.assertEquals(resultRows.get(1), new Object[]{"BB", 5.0});
    } else {
      Assert.assertEquals(resultRows.get(0), new Object[]{"BB", 5.0});
      Assert.assertEquals(resultRows.get(1), new Object[]{"Aa", 1.0});
    }
  }

  @Test(expectedExceptions = BadQueryRequestException.class, expectedExceptionsMessageRegExp = ".*average.*")
  public void shouldThrowOnUnknownAggFunction() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(
        new RexExpression.FunctionCall(SqlKind.AVG, FieldSpec.DataType.INT, "AVERAGE", ImmutableList.of()));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    DataSchema outSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});
    DataSchema inSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});

    // When:
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, inSchema, calls, group,
            AggType.INTERMEDIATE, null, null);
  }

  @Test
  public void shouldReturnErrorBlockOnUnexpectedInputType() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        // TODO: it is necessary to produce two values here, the operator only throws on second
        // (see the comment in Aggregate operator)
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, inSchema, calls, group,
            AggType.INTERMEDIATE, null, null);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Assert.assertTrue(block.isErrorBlock(), "expected ERROR block from invalid computation");
    Assert.assertTrue(block.getDataBlock().getExceptions().get(1000).contains("String cannot be cast to class"),
        "expected it to fail with class cast exception");
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(SqlKind.SUM, FieldSpec.DataType.INT, "SUM", ImmutableList.of(arg));
  }
}
