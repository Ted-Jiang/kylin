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
    * {
        box-sizing: border-box;
    }
    a:hover,
    a:focus {
        color: #23527c;
        text-decoration: underline;
    }
    a:focus {
        outline: 5px auto -webkit-focus-ring-color;
        outline-offset: -2px;
    }
</style>

<body>
    <div style="font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
        <hr style="margin-top: 10px; margin-bottom: 10px; height:0px; border-top: 1px solid #eee; border-right:0px; border-bottom:0px; border-left:0px;">
        <span style="display: inline; background-color: #337ab7; color: #fff; line-height: 1; font-weight: 700;font-size:36px; text-align: center;">&nbsp;Info&nbsp;</span>
        <hr style="margin-top: 10px; margin-bottom: 10px; height:0px; border-top: 1px solid #eee; border-right:0px; border-bottom:0px; border-left:0px;">
        <#if need_clean == true>
            <span style="line-height: 1;font-size: 16px;">
                <p style="text-align:left;">Hi Admin,</p>
                <p>There are ${cube_num} cubes will be retired for ${env}. Please clean them on ${retired_date}.</p>
            </span>
            <table frame="box" border="1px" cellpadding="0" cellspacing="0" width="100%" style="border-collapse: collapse;border:solid 1px  #245580;table-layout:fixed; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                <tr>
                    <td width="30%" style="padding: 10px; background-color: #337ab7; border: 1px solid #245580; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 12px; color: inherit; color: #fff; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            Cube Name
                        </h4>
                    </td>
                    <td width="30%" style="padding: 10px; background-color: #337ab7; border: 1px solid #245580; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 12px; color: inherit; color: #fff; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            Project Name
                        </h4>
                    </td>
                    <td width="20%" style="padding: 10px; background-color: #337ab7; border: 1px solid #245580; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 12px; color: inherit; color: #fff; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            Last Build Date
                        </h4>
                    </td>
                    <td width="20%" style="padding: 10px; background-color: #337ab7; border: 1px solid #245580; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 12px; color: inherit; color: #fff; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            Last Query Date
                        </h4>
                    </td>
                    <td width="20%" style="padding: 10px; background-color: #337ab7; border: 1px solid #245580; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 12px; color: inherit; color: #fff; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            Cube Region Count
                        </h4>
                    </td>
                    <td width="20%" style="padding: 10px; background-color: #337ab7; border: 1px solid #245580; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 12px; color: inherit; color: #fff; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            Cube Size(GB)
                        </h4>
                    </td>
                    <td width="20%" style="padding: 10px; background-color: #337ab7; border: 1px solid #245580; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 12px; color: inherit; color: #fff; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            Cube Owner
                        </h4>
                    </td>
                </tr>
                <#list cubes as cube>
                    <tr>
                        <td width="30%" style="padding: 10px; border: 1px solid #ddd; white-space:nowrap; overflow:scroll;">
                            <h4 style="margin-top: 0; margin-bottom: 0; font-size: 10px; color: inherit; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                ${cube.name}
                            </h4>
                        </td>
                        <td width="30%" style="padding: 10px; border: 1px solid #ddd; white-space:nowrap; overflow:scroll;">
                            <h4 style="margin-top: 0; margin-bottom: 0; font-size: 10px; color: inherit; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                ${cube.project}
                            </h4>
                        </td>
                        <td width="20%" style="padding: 10px; border: 1px solid #ddd; white-space:nowrap; overflow:scroll;">
                            <h4 style="margin-top: 0; margin-bottom: 0; font-size: 10px; color: inherit; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                ${cube.last_build_date}
                            </h4>
                        </td>
                        <td width="20%" style="padding: 10px; border: 1px solid #ddd; white-space:nowrap; overflow:scroll;">
                            <h4 style="margin-top: 0; margin-bottom: 0; font-size: 10px; color: inherit; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                                ${cube.last_query_date}
                            </h4>
                        </td>
                        <td width="20%" style="padding: 10px; border: 1px solid #ddd; white-space:nowrap; overflow:scroll;">
                          <h4 style="margin-top: 0; margin-bottom: 0; font-size: 10px; color: inherit; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                              ${cube.region_count}
                          </h4>
                      </td>
                      <td width="20%" style="padding: 10px; border: 1px solid #ddd; white-space:nowrap; overflow:scroll;">
                        <h4 style="margin-top: 0; margin-bottom: 0; font-size: 10px; color: inherit; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                            ${cube.cube_sizeGB}
                        </h4>
                      </td>
                    <td width="20%" style="padding: 10px; border: 1px solid #ddd; white-space:nowrap; overflow:scroll;">
                      <h4 style="margin-top: 0; margin-bottom: 0; font-size: 10px; color: inherit; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                          ${cube.cube_owner}
                      </h4>
                    </td>
                    </tr>
                </#list>
            </table>
        <#else>
            <span style="line-height: 1;font-size: 16px;">
                <p style="text-align:left;">Hi Admin,</p>
                <p>There is no cube should be clean for ${env}.</p>
            </span>
        </#if>
    </div>
</body>

</html>
