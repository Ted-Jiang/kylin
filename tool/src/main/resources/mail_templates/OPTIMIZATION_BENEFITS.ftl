<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">

<head>
    <meta http-equiv="Content-Type" content="Multipart/Alternative; charset=UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
</head>

<style type="text/css">
    .tftable {
        font-size: 12px;
        color: #333333;
        width: 100%;
        border-width: 1px;
        border-color: #bce8f1;
        border-collapse: collapse;
    }

    .tftable th {
        font-size: 12px;
        border-width: 1px;
        padding: 8px;
        border-style: solid;
        border-color: #bce8f1;
        text-align: left;
    }

    .tftable tr {
        background-color: #fff;
    }

    .tftable td {
        font-size: 12px;
        border-width: 1px;
        padding: 8px;
        border-style: solid;
        border-color: #bce8f1;
    }
</style>

<body>
<hr/>
<h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 16px;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
    The formula of cube optimization benefit :
</h4>

<table style="font-size: 12px">
    <tr>
        <td>queryBenefit</td>
        <td>=</td>
        <td>(rollupBenefit - rollupCost) / rollupInputCount</td>
    </tr>
    <tr>
        <td>spaceBenefit</td>
        <td>=</td>
        <td>(curTotalSize - recomTotalSize) / spaceLimit</td>
    </tr>
    <tr>
        <td>totalBenefit</td>
        <td>=</td>
        <td>queryBenefit + k * spaceBenefit</td>
    </tr>
    <tr>
        <td>score</td>
        <td>=</td>
        <td>1 / (1 + totalBenefit)</td>
    </tr>
</table>

<hr/>

<h4 style="margin-top: 10px;
margin-bottom: 0;
font-size: 16px;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
    Details for cube optimization
</h4>

<table class="tftable" border="1">

    <tr>
        <th bgcolor="#d9edf7">project name</th>
        <th bgcolor="#d9edf7">cube</th>
        <th bgcolor="#d9edf7"><font color="#ff8c00">score</font></th>
        <th bgcolor="#d9edf7">totalBenefit</th>
        <th bgcolor="#d9edf7">queryBenefit</th>
        <th bgcolor="#d9edf7">spaceBenefit</th>
        <th bgcolor="#d9edf7">rollupBenefit</th>
        <th bgcolor="#d9edf7">rollupCost</th>
        <th bgcolor="#d9edf7">rollupInputCount</th>
        <th bgcolor="#d9edf7">curTotalSize</th>
        <th bgcolor="#d9edf7">recomTotalSize</th>
        <th bgcolor="#d9edf7">spaceLimit</th>
        <th bgcolor="#d9edf7">k</th>
    </tr>
    <#list cubeBenefitList as benefit>
        <tr>
            <td>${benefit.projectName}</td>
            <td>${benefit.cube}</td>
            <td>${benefit.score}</td>
            <td>${benefit.totalBenefit}</td>
            <td>${benefit.queryBenefit}</td>
            <td>${benefit.spaceBenefit}</td>
            <td>${benefit.rollupBenefit}</td>
            <td>${benefit.rollupCost}</td>
            <td>${benefit.rollupInputCount}</td>
            <td>${benefit.curTotalSize}</td>
            <td>${benefit.recomTotalSize}</td>
            <td>${benefit.spaceLimit}</td>
            <td>${benefit.k}</td>
        </tr>
    </#list>


</table>
</body>

</html>

