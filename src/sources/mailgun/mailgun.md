{% docs mailgun_events %}

Union tables of the [Mailgun](https://www.mailgun.com/) events types:
1. events_rejected
1. events_delivered
1. events_failed
1. events_opened
1. events_clicked
1. events_unsubscribe
1. events_complained

Additionally, the messages with the below subjects have been excluded from the RAW dataset:

* `Managing users in your subscription`
* `Additional charges for your GitLab subscription`
* `Your GitLab subscription has been reconciled`

{% enddocs %}


{% docs mailgun_domains %}

Mailgun domains:
1. `customers.gitlab.com`
1. `mg.release.gitlab.net`
1. `gitlab.io`
1. `users.noreply.gitlab.com`

{% enddocs %}