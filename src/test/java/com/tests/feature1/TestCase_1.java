package com.tests.feature1;

import com.artos.annotation.TestCase;
import com.artos.annotation.TestPlan;
import com.artos.annotation.Unit;
import com.artos.framework.infra.TestContext;
import com.artos.interfaces.TestExecutable;
import org.junit.Assert;

@TestPlan(preparedBy = "arpit", preparationDate = "19/02/2019", bdd = "GIVEN..WHEN..AND..THEN..")
@TestCase
public class TestCase_1 implements TestExecutable {

	@Unit(sequence = 1)
	public void TC_Sign_In_05_05(TestContext context) {
		// --------------------------------------------------------------------------------------------
		context.getLogger().info("fgfgf");
		// --------------------------------------------------------------------------------------------
		System.out.println("This is testUnit_1");
		Assert.assertTrue(true);

	}

	@Unit(sequence = 2)
	public void TC_Sign_In_05_06(TestContext context) {
		// --------------------------------------------------------------------------------------------
		context.getLogger().info("fgfgf");
		// --------------------------------------------------------------------------------------------
		System.out.println("This is testUnit_2");
		Assert.assertTrue(true);

	}

	@Unit(sequence = 3)
	public void TC_Sign_In_05_07(TestContext context) {
		// --------------------------------------------------------------------------------------------
		context.getLogger().info("fgfgf");
		// --------------------------------------------------------------------------------------------
		System.out.println("This is testUnit_3");
		Assert.assertTrue(true);

	}

	@Unit(sequence = 4)
	public void TC_Sign_In_05_08(TestContext context) {
		// --------------------------------------------------------------------------------------------
		context.getLogger().info("fgfgf");
		// --------------------------------------------------------------------------------------------
		System.out.println("This is testUnit_4");
		Assert.assertTrue(false);

	}

}
