package mainboilerplate

import "github.com/jessevdk/go-flags"

// AddCommandFunc are used to register sub-commands with a parent
type AddCommandFunc func(*flags.Command) error

// CommandRegistry is a simple tool for building a tree of github.com/jessevdk/go-flags.AddCommand functions
// that you can use to register sub-commands under a github.com/jessevdk/go-flags.Command
type CommandRegistry map[string][]AddCommandFunc

// NewCommandRegistry creates a new registry
func NewCommandRegistry() CommandRegistry {
	return make(CommandRegistry)
}

// AddCommand takes a parentName and then an github.com/jessevdk/go-flags.AddCommand specification and stores it in the registry
// You can specify a tree of commands by separating parentName with dots.
// Example for adding command level1 and then level1 level2
//
//	AddCommand("level1",....)
//	AddCommand("level1.level2",....)
func (cr CommandRegistry) AddCommand(parentName string, command string, shortDescription string, longDescription string, data interface{}) {
	cr[parentName] = append(cr[parentName], func(cmd *flags.Command) error {
		_, err := cmd.AddCommand(command, shortDescription, longDescription, data)
		return err
	})
}

// AddCommands recursively walks through the tree of registered sub-commands under rootName and adds them under rootCmd.
// If recursive is true it will recurse down the tree to add sub-commands of sub-commands
func (cr CommandRegistry) AddCommands(rootName string, rootCmd *flags.Command, recursive bool) error {

	// Register any command
	for _, addCommandFunc := range cr[rootName] {
		if err := addCommandFunc(rootCmd); err != nil {
			return err
		}
	}

	// See if there are any sub-commands that now need registered under this one
	if recursive {
		for _, cmd := range rootCmd.Commands() {
			// Register every sub-command. Sub-Commands are separated with a .
			cmdName := cmd.Name
			if rootName != "" {
				cmdName = rootName + "." + cmdName
			}
			// Recursively register any sub-commands under this command
			if err := cr.AddCommands(cmdName, cmd, recursive); err != nil {
				return err
			}
		}
	}

	return nil

}
