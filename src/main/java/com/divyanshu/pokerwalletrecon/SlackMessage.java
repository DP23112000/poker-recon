package com.divyanshu.pokerwalletrecon;

import lombok.*;

@AllArgsConstructor
@Data
public class SlackMessage {

    private final String channel;
    private final String iconEmoji;
    private final String text;
    private final String username;

    SlackMessage(Builder builder) {
        this.channel = builder.channel;
        this.iconEmoji = builder.iconEmoji;
        this.text = builder.text;
        this.username = builder.username;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String channel;
        private String iconEmoji;
        private String text;
        private String username;

        public Builder channel(String channel) {
            this.channel = channel;
            return this;
        }

        public Builder iconEmoji(String iconEmoji) {
            this.iconEmoji = iconEmoji;
            return this;
        }

        public Builder text(String text) {
            this.text = text;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public SlackMessage build() {
            return new SlackMessage(this);
        }
    }

    public String getChannel() {
        return channel;
    }

    public String getIconEmoji() {
        return iconEmoji;
    }

    public String getText() {
        return text;
    }

    public String getUsername() {
        return username;
    }
}