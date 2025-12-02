package cmds

import (
	"os"

	"github.com/bmeg/benchtop/cmdline/benchtop/cmds/get"
	"github.com/bmeg/benchtop/cmdline/benchtop/cmds/keys"
	"github.com/bmeg/benchtop/cmdline/benchtop/cmds/tables"

	"github.com/spf13/cobra"
)

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:           "benchtop",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	RootCmd.AddCommand(keys.Cmd)
	RootCmd.AddCommand(tables.Cmd)
	RootCmd.AddCommand(get.Cmd)

	RootCmd.AddCommand(genBashCompletionCmd)
}

var genBashCompletionCmd = &cobra.Command{
	Use:   "bash",
	Short: "Generate bash completions file",
	Run: func(cmd *cobra.Command, args []string) {
		RootCmd.GenBashCompletion(os.Stdout)
	},
}
