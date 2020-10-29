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
    <span style="display: inline; background-color: #f0ad4e; color: #fff; line-height: 1; font-weight: 700;font-size:36px; text-align: center;">&nbsp;Warn&nbsp;</span>
    <hr style="margin-top: 10px; margin-bottom: 10px; height:0px; border-top: 1px solid #eee; border-right:0px; border-bottom:0px; border-left:0px;">
    <span style="line-height: 1;font-size: 16px;">
        <p style="text-align:left;">Dear Customer,</p>
        <p>This notice is to inform you that your cube will be retired due to inactive since ${inactive_days} days ago. The cube will be deleted in ${retired_day} days, please build/query this cube or contact kylin team if you want to keep it.</p>
    </span>
    <table cellpadding="0" cellspacing="0" width="100%" style="border-collapse: collapse;border:1px solid #faebcc;">
        <tr>
            <td style="padding: 10px 15px; background-color: #fcf8e3; border:1px solid #faebcc;">
                <h4 style="margin-top: 0; margin-bottom: 0; font-size: 14px; color: #8a6d3b; font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                Retired Cube Info
                </h4>
            </td>
        </tr>
        <tr>
            <td style="padding: 15px;">
                <table cellpadding="0" cellspacing="0" width="100%" style="margin-bottom: 20px;border:1 solid #ddd;border-collapse: collapse;font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal;">Enviroment</h4>
                        </th>
                        <td style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal; font-weight: 300;">
                                ${env}
                            </h4>
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal;">Cube</h4>
                        </th>
                        <td style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal; font-weight: 300;">
                                ${cube_name}
                            </h4>
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal;">Last Build Date</h4>
                        </th>
                        <td style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal; font-weight: 300;">
                                ${last_build_date}
                            </h4>
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal;">Last Query Date</h4>
                        </th>
                        <td style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal; font-weight: 300;">
                                ${last_query_date}</h4>
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal;">Retired Date</h4>
                        </th>
                        <td style="border: 1px solid #ddd; padding: 8px;">
                            <h4 style="margin-top: 0; margin-bottom: 0; line-height: 1.5; text-align: left; font-size: 14px; font-style: normal; font-weight: 300;">
                                ${retired_date}
                            </h4>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    <hr style="margin-top: 20px; margin-bottom: 20px; height:0px; border-top: 1px solid #eee; border-right:0px; border-bottom:0px; border-left:0px;">
    <h4 style="font-weight: 500; line-height: 1;font-size:16px;">
        <p>Best Wishes!</p>
        <p style="margin: 0 0 10px;">
            <a href="mailto:DL-eBay-Kylin-Core@ebay.com" style="color: #337ab7;text-decoration: none;">eBay ADI Kylin Team</a>
        </p>
    </h4>
</div>
</body>

</html>
