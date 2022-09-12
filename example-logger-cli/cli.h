/* SPDX-License-Identifier: Proprietary
 *
 * Yunhe Enmo (Beijing) Information Technology Co., Ltd.
 * Copyright (c) 2020 , All rights reserved.
 *
 */

#ifndef __CLI_H__
#define __CLI_H__

int cli_printf(char *fmt, ...);
void cli_cmd_end(void);
int cli_reg_cmd(char *cmd, char *help, void (*func)(int, char **));
void init_cli_server(const char *sock_path);
void exit_cli_server(void);

#endif //__CLI_H__
