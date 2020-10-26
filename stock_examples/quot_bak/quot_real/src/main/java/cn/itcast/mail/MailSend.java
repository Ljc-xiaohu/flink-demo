package cn.itcast.mail;

import javax.mail.Session;

/**
 * 测试类
 * 发送帐号用户:mailtestsender
 * 发送帐号密码:itcast2020
 * 发送帐号授权码UBJWJAECFMVKEYSW
 * 接收账户用户:mailtestreceiver
 * 接收账户密码:itcast2020
 * 发送帐号授权码SXTGGQZCHQFTLDZJ
 */
public class MailSend {
    /**
     * @param info 包含警告信息描述
     */
    public static void send(String info){
        String host = "smtp.163.com";//网易163邮件传输协议     腾讯 qq的是smtp.qq.com
        /**
         * 这里需要注意一下  如果你想用qq邮箱作为发件人邮箱的话  记得把邮箱传输协议host值改为smtp.qq.com
         * 另外 username登陆名还是 一样  直接写QQ号，不用加后缀
         */
        String username = "mailtestsender";//发件人邮箱的用户名 这里不要加后缀@163.com

        /** 注意事项
         * 这里的password不能写邮箱的登陆密码 需要去登录到邮箱 点 设置>账户
         * 选择"POP3/IMAP/SMTP/Exchange/CardDAV/CalDAV服务"选项，
         * 选择开启"POP3/SMTP服务"
         * 会提示你 或 系统生成一串授权码
         * 设置独立密码或使用授权码作为下面的password
         */
        //授权码
        String password = "UBJWJAECFMVKEYSW";  //发件人邮箱的客户端授权码

        /**
         * 这里发件人 要写全名
         */
        String from = "mailtestsender@163.com";//发件人的邮箱 全名 加后缀

        /**
         * 收件人 同样要写全名
         */
        String to = "mailtestreceiver@163.com";//收件人的邮箱

        /**
         * 主题自定义
         */
        String subject = "FlinkCEP实时预警-";//邮件主题
        /**
         * 自定义
         */
        //String content = "测试成功";//邮件的内容

        /**
         * 调用写好的邮件帮助类 MailUtils  直接调用createSession 根据以上（host, username, password三个参数）创建出session
         */
        Session session = MailUtils.createSession(host, username, password);
        /**
         * 创建邮件对象from, to,subject,content 这三个参数
         */
        MailBean mail = new MailBean(from, to,subject,info);
        try {
            /**
             * 最后一步  调用MailUtils的send方法 将session和创建好的邮件对象传进去  发送就ok了
             */
            MailUtils.send(session, mail);
            System.out.println("实时预警邮件已发送");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
