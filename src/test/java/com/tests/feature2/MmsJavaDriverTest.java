package com.tests.feature2;

import com.artos.annotation.TestCase;
import com.artos.annotation.Unit;
import com.artos.framework.infra.TestContext;
import com.artos.interfaces.TestExecutable;
import net.sourceforge.marathon.javadriver.JavaDriver;
import net.sourceforge.marathon.javadriver.JavaProfile;
import net.sourceforge.marathon.javadriver.JavaProfile.LaunchMode;
import net.sourceforge.marathon.javadriver.JavaProfile.LaunchType;
import org.apache.commons.validator.routines.checkdigit.SedolCheckDigit;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.util.List;
import java.util.Set;

import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
//import org.testng.annotations.*;

import io.appium.java_client.windows.WindowsDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.DesiredCapabilities;
//import org.testng.annotations.*;

import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@TestCase
public class MmsJavaDriverTest implements TestExecutable {

    @Unit
    public void controlDespatchLaunchedFromMMS(TestContext context) throws Exception {

        JavaProfile profile = new JavaProfile(LaunchMode.COMMAND_LINE);
        profile.setLaunchType(LaunchType.SWING_APPLICATION);
        profile.setCommand("C:\\Kiwiplan\\client\\bin\\KiwiplanLauncher.exe");
        profile.setWorkingDirectory("C:\\Kiwiplan\\client\\bin");

        profile.setStartWindowTitle("Login");

        WebDriver driver = new JavaDriver(profile);

        WebDriverWait webDriverWait = new WebDriverWait(driver,15);

        driver.switchTo().window("Login");

        System.out.println("Window Title: " + driver.getTitle());

        String curWinHandle = driver.getWindowHandle();

        Set<String> windowHandles = driver.getWindowHandles();

        for (String winHandle : windowHandles) {

            System.out.println("Window handles:" + winHandle);

        }

//        List<WebElement> textFieldElements = driver.findElements(By.cssSelector("text-field"));

        List<WebElement> textFieldElements = webDriverWait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(By.cssSelector("text-field")));

        textFieldElements.get(0).sendKeys("admin");

        WebElement passwordElem = driver.findElement(By.tagName("password-field"));

        passwordElem.sendKeys("leoleo");

        Thread.sleep(2000);

//        WebElement okButton = driver.findElement(By.cssSelector("button[text='OK']"));

        WebElement okButton = webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("button[text='OK']")));

        okButton.click();

        Thread.sleep(5000);

        driver.switchTo().window("Material Management");

        System.out.println("Window Title: " + driver.getTitle());

        Set<String> winHandlesAfterLogin = driver.getWindowHandles();

        for (String winHandleAfterLogin: winHandlesAfterLogin) {

            System.out.println("winHandleAfterLogin: " + winHandleAfterLogin);

        }

        Thread.sleep(5000);

//        WebElement MENU = driver.findElement(By.cssSelector("menu[text*='Window']"));

        WebElement menu = webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("menu[text*='Window']")));

        menu.click();

        Thread.sleep(2000);

//        WebElement MENU = driver.findElement(By.cssSelector("menu-item"));

        List<WebElement> menuItems = webDriverWait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(By.cssSelector("menu-item")));

        for (WebElement menuItem: menuItems) {

            System.out.println("MenuItem: " + menuItem.getText());

        }

        System.out.println("The end of menu items.");

        menuItems.get(1).click();

        Thread.sleep(5000);

        driver.switchTo().window("Kiwiplan for Planners - Plant 2");

//        driver.findElement(By.cssSelector("menu[text*='Reports']")).click();

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("menu[text*='Reports']"))).click();

        Thread.sleep(2000);

//        List<WebElement> reportMenuItems = driver.findElements(By.cssSelector("menu-item"));

        List<WebElement> reportMenuItems = webDriverWait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(By.cssSelector("menu-item")));

        for (WebElement reportMenuItem: reportMenuItems) {

            System.out.println("MenuItem: " + reportMenuItem.getText());

        }

        reportMenuItems.get(2).click();

//        WindowsDriver kpClientWindowDriver = getFocusOnWindowAndReturnWindowsDrvier("Despatch Release Module");

        WindowsDriver kpClientWindowDriver = getFocusOnWindowAndReturnWindowsDrvier("Despatch Release Module - Google Chrome");

        List<WebElement> editElements = kpClientWindowDriver.findElementsByXPath("//*[@LocalizedControlType='check box']");

        System.out.println("editElements size() " + editElements.size());

        editElements.get(1).click();

        Thread.sleep(2000);

        WebElement newLoadElement = kpClientWindowDriver.findElementByName("New Load");

        newLoadElement.click();

    }

    public WindowsDriver getFocusOnWindowAndReturnWindowsDrvier(String windowName) throws Exception {

        //Creates a desktop session.
        DesiredCapabilities desktopCapabilities = new DesiredCapabilities();
        desktopCapabilities.setCapability("app", "Root");
        desktopCapabilities.setCapability("platformName", "Windows");
        desktopCapabilities.setCapability("deviceName", "WindowsPC");
        WindowsDriver<WebElement> desktopWinDriver = new WindowsDriver<WebElement>(new URL("http://127.0.0.1:4723"), desktopCapabilities);
        desktopWinDriver.manage().timeouts().implicitlyWait(2, TimeUnit.SECONDS);

        WebDriverWait webDriverWait = new WebDriverWait(desktopWinDriver,10);

        WebElement windowElement = webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.name(windowName)));

        String windowHandleStr = windowElement.getAttribute("NativeWindowHandle");;

        System.out.println("NativeWindowHandle for Configuration Wizard:" + windowHandleStr);

        int windowHandleInt = Integer.parseInt(windowHandleStr);
        String windowHandleHex = "0x" + Integer.toHexString(windowHandleInt);

        System.out.println("windowHandleHex: " + windowHandleHex);

        //Creates a Window session.
        DesiredCapabilities windowCapabilities = new DesiredCapabilities();
        windowCapabilities.setCapability("platformName", "Windows");
        windowCapabilities.setCapability("deviceName", "WindowsPC");
        windowCapabilities.setCapability("appTopLevelWindow", windowHandleHex);
        WindowsDriver windowDriver = new WindowsDriver(new URL("http://127.0.0.1:4723"), windowCapabilities);
        windowDriver.manage().timeouts().implicitlyWait(2, TimeUnit.SECONDS);

        return windowDriver;

    }


    public void controlMmsClient() throws InterruptedException {

        JavaProfile profile = new JavaProfile(LaunchMode.COMMAND_LINE);
        profile.setLaunchType(LaunchType.SWING_APPLICATION);
        profile.setCommand("C:\\Kiwiplan\\Client\\9731_29\\bin\\KiwiplanLauncher.exe");
        profile.setWorkingDirectory("C:\\Kiwiplan\\Client\\9731_29\\bin");
        profile.setStartWindowTitle("Login");

        WebDriver driver = new JavaDriver(profile);
        driver.switchTo().window("Login");

        System.out.println("Window Title: " + driver.getTitle());

        String curWinHandle = driver.getWindowHandle();

        Set<String> windowHandles =  driver.getWindowHandles();

        for(String winHandle: windowHandles) {

            System.out.println("Window handles:" + winHandle);

        }

        List<WebElement> textFieldElements = driver.findElements(By.cssSelector("text-field"));

        textFieldElements.get(0).sendKeys("admin");

        WebElement passwordElem = driver.findElement(By.tagName("password-field"));

        passwordElem.sendKeys("leoleo");

        Thread.sleep(2000);

        WebElement okButton = driver.findElement(By.cssSelector("button[text='OK']"));

        okButton.click();

        Thread.sleep(5000);

        driver.switchTo().window("Material Management");

        System.out.println("Window Title: " + driver.getTitle());

        Set<String> winHandlesAfterLogin = driver.getWindowHandles();

        for (String winHandleAfterLogin: winHandlesAfterLogin) {

            System.out.println("winHandleAfterLogin: " + winHandleAfterLogin);

        }

        Thread.sleep(5000);

        WebDriverWait webDriverWait = new WebDriverWait(driver,15);

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("tabbed-pane::all-tabs[text='Dashboard']"))).click();

        Thread.sleep(3000);

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("tabbed-pane::all-tabs[text='Requirements']"))).click();

        Thread.sleep(3000);

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("tabbed-pane::all-tabs[text='Inventory Management']"))).click();

        Thread.sleep(3000);

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("tabbed-pane::all-tabs[text='Supplier Management']"))).click();

        Thread.sleep(3000);

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("tabbed-pane::all-tabs[text='Material Management']"))).click();

        Thread.sleep(3000);

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("tabbed-pane::all-tabs[text='Requirements']"))).click();

        Thread.sleep(3000);

        WebElement jobMmHeader = driver.findElement(By.cssSelector("table-header::all-items[text='Job']"));
        jobMmHeader.click();

        WebElement customerMmHeader = driver.findElement(By.cssSelector("table-header::all-items[text='Customer']"));
        customerMmHeader.click();

        Thread.sleep(2000);

        WebElement table = driver.findElement(By.cssSelector("table::all-cells"));
        System.out.println("rowCount: " + table.getAttribute("rowCount"));
        System.out.println("columnCount: " + table.getAttribute("columnCount"));

        webDriverWait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("text-field"))).sendKeys("90683\n");

        Thread.sleep(2000);

        List<WebElement> tablesDisplayed = driver.findElements(By.cssSelector("table:displayed"));
        List<WebElement> oneTwoCellsDisplayed = driver.findElements(By.cssSelector("table:displayed::mnth-cell(1,2)"));
        List<WebElement> oneThreeCellsDisplayed = driver.findElements(By.cssSelector("table:displayed::mnth-cell(1,3)"));

        System.out.println("The number of tables displayed: " + tablesDisplayed.size());
        for (WebElement tblDisplayed: tablesDisplayed) {

            System.out.println("Table displayed: " + tblDisplayed.getText());

        }
/*
        System.out.println("The number of oneOneCellsDisplayed: " + oneOneCellsDisplayed.size());
        for (WebElement oneOneCellDisplayed: oneOneCellsDisplayed) {

            System.out.println("oneOneCellDisplayed: " + oneOneCellDisplayed.getText());

        }

 */

        System.out.println("The number of oneTwoCellsDisplayed: " + oneTwoCellsDisplayed.size());
        for (WebElement oneTwoCellDisplayed: oneTwoCellsDisplayed) {

            System.out.println("oneTwoCellDisplayed: " + oneTwoCellDisplayed.getText());

        }

        System.out.println("The number of oneThreeCellsDisplayed: " + oneThreeCellsDisplayed.size());
        for (WebElement oneThreeCellDisplayed: oneThreeCellsDisplayed) {

            System.out.println("oneThreeCellDisplayed: " + oneThreeCellDisplayed.getText());

        }

        /*
        List<WebElement> tables = driver.findElements(By.cssSelector("table"));
        List<WebElement> oneOneCells = driver.findElements(By.cssSelector("table::mnth-cell(1,1)"));
        List<WebElement> oneTwoCells = driver.findElements(By.cssSelector("table::mnth-cell(1,2)"));
        List<WebElement> oneThreeCells = driver.findElements(By.cssSelector("table::mnth-cell(1,3)"));

        System.out.println("cell (1, 1): " + oneOneCells.get(1).getText());
        System.out.println("cell (1, 2): " + oneTwoCells.get(1).getText());

        System.out.println("The number of tables: " + tables.size());
        for (WebElement tbl: tables) {

            System.out.println("Table: " + tbl.getText());

        }

        System.out.println("The number of oneThreeCells: " + oneThreeCells.size());

        System.out.println("First oneThreeCell: " + oneThreeCells.get(0).getText());
        System.out.println("Second oneOneCell: " + oneOneCells.get(1).getText());
        System.out.println("Second oneTwoCell: " + oneTwoCells.get(1).getText());
        System.out.println("Six oneOneCell: " + oneOneCells.get(5).getText());
        System.out.println("Six oneTwoCell: " + oneTwoCells.get(5).getText());
*/

    }

}
