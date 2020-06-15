<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">

<head>
    <meta http-equiv="Content-Type" content="Multipart/Alternative; charset=UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
</head>

<style>
    html {
        font-size: 10px;
    }
</style>

<body>
<div style="font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">

    <hr style="margin-top: 10px;
margin-bottom: 10px;
height:0px;
border-top: 1px solid #eee;
border-right:0px;
border-bottom:0px;
border-left:0px;">
    <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 16px;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
        Summary
    </h4>
    <table frame="box" border="1px" cellpadding="0" cellspacing="0" width="100%"
           style="border-collapse: collapse;border:solid 1px  #ddd;table-layout:fixed; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
    <#list envRetList as env>
        <tr>
            <td width="40%" style="padding: 10px 0px;
border: 1px solid #ddd;
white-space:nowrap;
overflow:scroll;">
                <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    &nbsp;${env.env}
                </h4>
            </td>
            <td width="15%" style="padding: 10px 0px;
border: 1px solid #ddd;
white-space:nowrap;
overflow:scroll;">
                <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    &nbsp;${env.size} projects
                </h4>
            </td>
            <td width="15%" style="padding: 10px 0px;
border: 1px solid #ddd;
white-space:nowrap;
overflow:scroll;">
                <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    &nbsp;${env.regionCount} regions
                </h4>
            </td>
            <td width="15%" style="padding: 10px 0px;
border: 1px solid #ddd;
white-space:nowrap;
overflow:scroll;">
                <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    &nbsp;${env.tableCount} htables
                </h4>
            </td>
            <td width="15%" style="padding: 10px 0px;
    border: 1px solid #ddd;
    white-space:nowrap;
    overflow:scroll;">
                <h4 style="margin-top: 0;
    margin-bottom: 0;
    font-size: 12px;
    color: inherit;
    font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    &nbsp;${env.tableSize}
                </h4>
            </td>
        </tr>
    </#list>
    </table>

    <hr style="margin-top: 15px;
margin-bottom: 15px;
height:0px;
border-top: 1px solid #eee;
border-right:0px;
border-bottom:0px;
border-left:0px;">
    <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 16px;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
        Details
    </h4>
<#list envList as env>
    <div style="margin-bottom:5px">
        <table frame="box" border="1px" cellpadding="0" cellspacing="0" width="100%"
               style="border-collapse: collapse;border:solid 1px  #245580;table-layout:fixed; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
            <tr>
                <td width="40%" style="
padding: 10px 0px;
background-color: #337ab7;
border: 1px solid #245580;
white-space:nowrap;
overflow:scroll;">
                    <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
color: #fff;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                        &nbsp;${env.env}
                    </h4>
                </td>
                <td width="15%" style="
padding: 10px 0px;
background-color: #337ab7;
border: 1px solid #245580;
white-space:nowrap;
overflow:scroll;">
                    <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
color: #fff;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                        &nbsp;${env.size} projects
                    </h4>
                </td>
                <td width="15%" style="
padding: 10px 0px;
background-color: #337ab7;
border: 1px solid #245580;
white-space:nowrap;
overflow:scroll;">
                    <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
color: #fff;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                        &nbsp;${env.regionCount} regions
                    </h4>
                </td>
                <td width="15%" style="
padding: 10px 0px;
background-color: #337ab7;
border: 1px solid #245580;
white-space:nowrap;
overflow:scroll;">
                    <h4 style="margin-top: 0;
margin-bottom: 0;
font-size: 12px;
color: inherit;
color: #fff;
font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                        &nbsp;${env.tableCount} htables
                    </h4>
                </td>
                <td width="15%" style="
    padding: 10px 0px;
    background-color: #337ab7;
    border: 1px solid #245580;
    white-space:nowrap;
    overflow:scroll;">
                    <h4 style="margin-top: 0;
    margin-bottom: 0;
    font-size: 12px;
    color: inherit;
    color: #fff;
    font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                        &nbsp;${env.tableSize}
                    </h4>
                </td>
            </tr>

            <tr>
                <td style="padding: 5px;" colspan="5">
                    <#list env.projList as proj>
                        <table cellpadding="0" cellspacing="0" border="1px" width="100%"
                               style="margin-bottom: 20px;border:1px solid #bce8f1;border-collapse: collapse;table-layout:fixed;font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            <tr>
                                <td width="40%" style=" padding: 10px 0px;
        background-color: #d9edf7;
        border: 1px solid #bce8f1;
        white-space:nowrap;
        overflow:auto;">
                                    <h4 style="margin-top: 0;
        margin-bottom: 0;
        font-size: 12px;
        color: #31708f;
        font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                        &nbsp;${proj.proj}
                                    </h4>
                                </td>
                                <td width="15%" style="padding: 10px 0px;
    background-color: #d9edf7;
    border: 1px solid #bce8f1;
    white-space:nowrap;
    overflow:scroll;">
                                    <h4 style="margin-top: 0;
    margin-bottom: 0;
    font-size: 12px;
    color: inherit;
    color: #31708f;
    font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                        &nbsp;${proj.size} cubes
                                    </h4>
                                </td>
                                <td width="15%" style="padding: 10px 0px;
            background-color: #d9edf7;
            border: 1px solid #bce8f1;
            white-space:nowrap;
            overflow:scroll;">
                                    <h4 style="margin-top: 0;
            margin-bottom: 0;
            font-size: 12px;
            color: inherit;
            color: #31708f;
            font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                        &nbsp;${proj.regionCount} regions
                                    </h4>
                                </td>
                                <td width="15%" style="padding: 10px 0px;
    background-color: #d9edf7;
    border: 1px solid #bce8f1;
    white-space:nowrap;
    overflow:scroll;">
                                    <h4 style="margin-top: 0;
    margin-bottom: 0;
    font-size: 12px;
    color: inherit;
    color: #31708f;
    font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                        &nbsp;${proj.tableCount} htables
                                    </h4>
                                </td>
                                <td width="15%" style="padding: 10px 0px;
            background-color: #d9edf7;
            border: 1px solid #bce8f1;
            white-space:nowrap;
            overflow:scroll;">
                                    <h4 style="margin-top: 0;
            margin-bottom: 0;
            font-size: 12px;
            color: inherit;
            color: #31708f;
            font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                        &nbsp;${proj.tableSize}
                                    </h4>
                                </td>
                            </tr>

                            <tr>
                                <td style="padding: 5px;" colspan="5">
                                    <table cellpadding="0" cellspacing="0" width="100%" border="1px"
                                           style="margin-bottom: 20px;border:1 solid #ddd;border-collapse: collapse;font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                        <#list proj.cubeList as cube>
                                            <tr>
                                                <td width="55%" style="padding: 10px 0px;
                            border: 1px solid #ddd;
                            white-space:nowrap;
                            overflow:scroll;">
                                                    <h4 style="margin-top: 0;
                            margin-bottom: 0;
                            font-size: 10px;
                            color: inherit;
                            font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                                        &nbsp;${cube.cube}
                                                    </h4>
                                                </td>
                                                <td width="15%" style="padding: 10px 0px;
                    border: 1px solid #ddd;
                    white-space:nowrap;
                    overflow:scroll;">
                                                    <h4 style="margin-top: 0;
                    margin-bottom: 0;
                    font-size: 10px;
                    color: inherit;
                    font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                                        &nbsp;${cube.regionCount} regions
                                                    </h4>
                                                </td>
                                                <td width="15%" style="padding: 10px 0px;
                            border: 1px solid #ddd;
                            white-space:nowrap;
                            overflow:scroll;">
                                                    <h4 style="margin-top: 0;
                            margin-bottom: 0;
                            font-size: 10px;
                            color: inherit;
                            font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                                        &nbsp;${cube.tableCount} htables
                                                    </h4>
                                                </td>
                                                <td width="15%" style="padding: 10px 0px;
                            border: 1px solid #ddd;
                            white-space:nowrap;
                            overflow:scroll;">
                                                    <h4 style="margin-top: 0;
                            margin-bottom: 0;
                            font-size: 10px;
                            color: inherit;
                            font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                                        &nbsp;${cube.tableSize}
                                                    </h4>
                                                </td>
                                            </tr>
                                        </#list>
                                    </table>
                                </td>
                            </tr>
                        </table>
                    </#list>
                </td>
            </tr>
        </table>
    </div>
</#list>
</div>
</body>

</html>