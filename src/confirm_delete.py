from discord import ui, ButtonStyle
import discord

class ConfirmDeleteView(ui.View):
    def __init__(self, confirm_label, cancel_label, *, timeout=60):
        super().__init__(timeout=timeout)
        self.value = None

        # Create confirm button
        confirm_button = ui.Button(label=confirm_label, style=ButtonStyle.red, custom_id='confirm_delete')
        confirm_button.callback = self.confirm
        self.add_item(confirm_button)

        # Create cancel button
        cancel_button = ui.Button(label=cancel_label, style=ButtonStyle.grey, custom_id='cancel_delete')
        cancel_button.callback = self.cancel

    async def confirm(self, interaction: discord.Interaction):
        self.value = True
        self.stop()
        await interaction.response.defer(ephemeral=True)

    async def cancel(self, interaction: discord.Interaction):
        self.value = False
        self.stop()
        await interaction.response.defer(ephemeral=True)
